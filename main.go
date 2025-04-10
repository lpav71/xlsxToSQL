package main

import (
	"bufio"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"gorm.io/gorm/clause"
	"gorm.io/gorm/logger"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/xuri/excelize/v2"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
)

// Модель для хранения уникальных записей
type Product struct {
	ID      uint   `gorm:"primaryKey;autoIncrement"`                                                  // BIGINT(20) UNSIGNED NOT NULL AUTO_INCREMENT
	Article string `gorm:"type:varchar(255);not null;collate:utf8mb4_unicode_ci;index:article_brand"` // VARCHAR(255) NOT NULL
	Brand   string `gorm:"type:varchar(255);not null;collate:utf8mb4_unicode_ci;index:article_brand"` // VARCHAR(255) NOT NULL
	Name    string `gorm:"type:varchar(255);not null;collate:utf8mb4_unicode_ci"`                     // VARCHAR(255) NOT NULL
	Hash    string `gorm:"type:varchar(64);not null;unique;collate:utf8mb4_unicode_ci"`               // VARCHAR(64) NOT NULL UNIQUE
}

// TableName Указывает имя таблицы (опционально)
func (Product) TableName() string {
	return "products"
}

var mu sync.Mutex

func main() {
	// Подключение к временной MySQL базе для обработки данных
	dsn := "root:1234@tcp(127.0.0.1:3306)/testdb?charset=utf8mb4&parseTime=True&loc=Local"
	db, err := gorm.Open(mysql.Open(dsn), &gorm.Config{})
	if err != nil {
		log.Fatalf("Не удалось подключиться к базе данных: %v", err)
	}

	// Настройка пула соединений
	sqlDB, err := db.DB()
	if err != nil {
		log.Fatal("не удалось получить доступ к базовым соединениям:", err)
	}
	sqlDB.SetMaxOpenConns(50)                 // Максимум 50 открытых соединений
	sqlDB.SetMaxIdleConns(20)                 // Максимум 20 простаивающих соединений
	sqlDB.SetConnMaxLifetime(time.Minute * 5) // Время жизни соединения

	db.Logger = logger.Default.LogMode(logger.Silent)

	startTime := time.Now() // Запоминаем начальное время

	// Создание таблицы, если её нет
	err = db.AutoMigrate(&Product{})
	if err != nil {
		log.Fatalf("Не удалось создать таблицу: %v", err)
	}

	// Чтение всех xlsx файлов из директории
	dirPath := "./prices" // Путь к директории с файлами
	files, err := os.ReadDir(dirPath)
	if err != nil {
		log.Fatalf("Не удалось прочитать директорию: %v", err)
	}

	var wg sync.WaitGroup // Группа ожидания для управления горутинами

	for _, file := range files {
		if filepath.Ext(file.Name()) == ".xlsx" {
			wg.Add(1) // Добавляем задачу в группу ожидания
			go func(filePath string) {
				defer wg.Done() // Отмечаем задачу как выполненную после завершения
				processXLSXFile(db, filePath)
			}(filepath.Join(dirPath, file.Name()))
		}
	}

	// Ждём завершения всех горутин
	wg.Wait()

	// Экспорт данных в SQL файл
	exportToSQLFile(db, "output.sql")

	// Очистка таблицы перед завершением
	//if err := clearTable(db, "products"); err != nil {
	//	log.Fatalf("Ошибка при очистке таблицы: %v", err)
	//}

	elapsedTime := time.Since(startTime) // Вычисляем время выполнения
	fmt.Printf("Время выполнения (форматированный вывод): %.2f секунд\n", elapsedTime.Seconds())
	fmt.Println("Время выполнения (стандарный вывод):", elapsedTime)
}

// Функция для очистки таблицы
func clearTable(db *gorm.DB, tableName string) error {
	return db.Exec(fmt.Sprintf("TRUNCATE TABLE `%s`", tableName)).Error
}

// Глубокая очистка строки от всех нежелательных символов
func deepClean(value string) string {
	// Удаляем все пробельные символы (включая табуляции и переносы строк)
	value = strings.TrimSpace(value)
	value = strings.ReplaceAll(value, "\t", "")
	value = strings.ReplaceAll(value, "\n", "")
	value = strings.ReplaceAll(value, "\r", "")

	// Преобразуем в нижний регистр
	value = strings.ToLower(value)

	// Удаляем все специальные символы (оставляем только буквы и цифры)
	value = removeNonAlphanumeric(value)

	return value
}

// Удаление всех не буквенно-цифровых символов
func removeNonAlphanumeric(value string) string {
	result := ""
	for _, char := range value {
		if (char >= 'a' && char <= 'z') || (char >= '0' && char <= '9') {
			result += string(char)
		}
	}
	return result
}

// Генерация хэша для комбинации article + brand
func generateHash(article, brand string) string {
	article = deepClean(article)
	brand = deepClean(brand)
	hashInput := article + brand
	hash := sha256.Sum256([]byte(hashInput))
	return hex.EncodeToString(hash[:])
}

// Обработка одного xlsx файла
func processXLSXFile(db *gorm.DB, filePath string) {
	f, err := excelize.OpenFile(filePath)
	if err != nil {
		log.Printf("Не удалось открыть файл %s: %v\n", filePath, err)
		return
	}

	sheetList := f.GetSheetList()
	if len(sheetList) == 0 {
		log.Printf("Файл %s не содержит листов.\n", filePath)
		return
	}

	// Проходим по всем листам
	for _, currentSheet := range sheetList {
		rows, err := f.GetRows(currentSheet)
		if err != nil {
			log.Printf("Не удалось прочитать лист %s в файле %s: %v\n", currentSheet, filePath, err)
			return
		}

		for _, row := range rows {
			if len(row) < 3 {
				continue // Пропускаем строки, где недостаточно данных
			}

			brand := normalizeBrand(row[1])     // Нормализуем бренд
			article := normalizeArticle(row[2]) // Нормализуем артикул
			name := strings.TrimSpace(row[3])   // Очищаем название

			// Генерируем хэш для комбинации article + brand
			hash := generateHash(article, brand)

			// Ищем существующую запись по хэшу
			var existing Product
			if errors.Is(db.Where("hash = ?", hash).First(&existing).Error, gorm.ErrRecordNotFound) {
				// Проверяем, есть ли запись с такими же article и brand
				var duplicate Product
				if db.Where("article = ? AND brand = ?", article, brand).Find(&duplicate).RowsAffected > 0 {
					log.Printf("Найдена запись с такими же article и brand, но другим хэшем: id=%d, hash=%s, expected_hash=%s\n",
						duplicate.ID, duplicate.Hash, hash)
				}
			}

			// Если записи нет или новое название длиннее, обновляем запись
			if existing.ID == 0 || len(name) > len(existing.Name) {
				mu.Lock()
				db.Clauses(clause.OnConflict{
					Columns:   []clause.Column{{Name: "hash"}},
					DoUpdates: clause.Assignments(map[string]interface{}{"name": name}),
				}).Create(&Product{Article: article, Brand: brand, Name: name, Hash: hash})
				mu.Unlock()
			}
		}
	}

}

// Нормализация артикула (убираем специальные символы и преобразуем в нижний регистр)
func normalizeArticle(article string) string {
	specialChars := []string{"-", "_", ".", "/", "+", " ", ","}
	cleaned := strings.ToLower(strings.TrimSpace(article)) // Удаляем лишние пробелы
	for _, char := range specialChars {
		cleaned = strings.ReplaceAll(cleaned, char, "")
	}
	return cleaned
}

// Нормализация бренда (преобразуем в нижний регистр и удаляем пробелы)
func normalizeBrand(brand string) string {
	return strings.ToLower(strings.TrimSpace(brand))
}

// Экспорт данных в SQL файл
func exportToSQLFile(db *gorm.DB, outputPath string) {
	file, err := os.Create(outputPath)
	if err != nil {
		log.Fatalf("Не удалось создать SQL файл: %v", err)
	}
	defer file.Close()

	writer := bufio.NewWriterSize(file, 1<<20) // 1 MB буфер
	defer writer.Flush()

	// Записываем заголовок создания таблицы
	tableName := "products"
	writer.WriteString(fmt.Sprintf("CREATE TABLE IF NOT EXISTS `%s` (\n", tableName))
	writer.WriteString("`id` INT AUTO_INCREMENT PRIMARY KEY,\n")
	writer.WriteString("`article` VARCHAR(255) NOT NULL,\n")
	writer.WriteString("`brand` VARCHAR(255) NOT NULL,\n")
	writer.WriteString("`name` VARCHAR(255) NOT NULL\n")
	writer.WriteString(");\n\n")

	// Пагинация для выборки данных
	limit := 1000 // Количество записей за одну итерацию
	offset := 0
	for {
		var products []Product
		err := db.Limit(limit).Offset(offset).Find(&products).Error
		if err != nil {
			log.Fatalf("Ошибка при выборке данных: %v", err)
		}

		if len(products) == 0 {
			break // Все записи обработаны
		}

		// Генерируем INSERT запросы для текущей страницы
		for _, product := range products {
			writer.WriteString(fmt.Sprintf("INSERT INTO `%s` (`article`, `brand`, `name`) VALUES ('%s', '%s', '%s');\n",
				tableName, escapeSQL(product.Article), escapeSQL(product.Brand), escapeSQL(product.Name)))
		}

		offset += limit
	}
}

// Экранирование строк для SQL
func escapeSQL(value string) string {
	// Экранируем обратный слэш (\) сначала, так как он используется для других escape-символов
	value = strings.ReplaceAll(value, "\\", "\\\\")
	// Экранируем одинарные кавычки (') путем удвоения
	value = strings.ReplaceAll(value, "'", "''")
	// Если необходимо, можно добавить обработку других специальных символов
	return value
}
