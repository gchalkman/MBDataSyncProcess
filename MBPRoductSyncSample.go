package main

import (
	"bytes"
	"database/sql"
	"encoding/xml"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"mime/multipart"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/chromedp/chromedp"
	"github.com/mattn/go-sqlite3"
	_ "github.com/mattn/go-sqlite3"
	"golang.org/x/net/context"
)

const (
	folderPath  = "./product"
	dbFileName  = "products.db"
	datasetGUID = "guid"
	authToken   = "token"
	maxWorkers  = 5
	maxRetries  = 5
)

type RSS struct {
	Channel Channel `xml:"channel"`
}

type Channel struct {
	Items []Item `xml:"item"`
}

type Item struct {
	ID           string  `xml:"id"`
	Title        string  `xml:"title"`
	Description  string  `xml:"description"`
	Price        float64 `xml:"price"`
	Link         string  `xml:"link"`
	ImageLink    string  `xml:"image_link"`
	Brand        string  `xml:"brand"`
	MPN          string  `xml:"mpn"`
	GTIN         string  `xml:"gtin"`
	Availability string  `xml:"availability"`
	Condition    string  `xml:"condition"`
	Inventory    int     `xml:"inventory"`
}

type Product struct {
	UniqueCode string
	Price      float64
	MPN        string
	Status     string
}

var dbMutex sync.Mutex

// fileExists checks if a file exists at the given path.
func fileExists(filePath string) bool {
	_, err := os.Stat(filePath)
	if err == nil {
		return true // File exists
	}
	if os.IsNotExist(err) {
		return false // File does not exist
	}
	return false // Some other error occurred
}

// uploadFile sends a POST request to upload a file to a remote server.
// uploadFile sends a POST request to upload a file to a remote server.
func uploadFile(filePath string) error {
	url := fmt.Sprintf("https://b2b.my-buddy.ai/v1/datasets/%s/document/create_by_file", datasetGUID)
	payload := `{"indexing_technique":"high_quality","process_rule":{"rules":{"pre_processing_rules":[{"id":"remove_extra_spaces","enabled":true},{"id":"remove_urls_emails","enabled":false}],"segmentation":{"separator":"###","max_tokens":1000}},"mode":"custom"}}`

	file, err := os.Open(filePath)
	if err != nil {
		return fmt.Errorf("failed to open file %s: %v", filePath, err)
	}
	defer file.Close()

	body := &bytes.Buffer{}
	writer := multipart.NewWriter(body)
	part, err := writer.CreateFormFile("file", filepath.Base(file.Name()))
	if err != nil {
		return fmt.Errorf("failed to create form file: %v", err)
	}

	_, err = io.Copy(part, file)
	if err != nil {
		return fmt.Errorf("failed to copy file content: %v", err)
	}

	err = writer.WriteField("data", payload)
	if err != nil {
		return fmt.Errorf("failed to write payload data: %v", err)
	}

	err = writer.Close()
	if err != nil {
		return fmt.Errorf("failed to close writer: %v", err)
	}

	req, err := http.NewRequest("POST", url, body)
	if err != nil {
		return fmt.Errorf("failed to create upload request: %v", err)
	}

	req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", authToken))
	req.Header.Set("Content-Type", writer.FormDataContentType())

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("failed to execute upload request: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		bodyBytes, _ := ioutil.ReadAll(resp.Body)
		return fmt.Errorf("failed to upload file: %d - %s", resp.StatusCode, string(bodyBytes))
	}

	fmt.Printf("File %s uploaded successfully\n", filePath)
	return nil
}

// markAllRecordsAsDeleted updates the status of all records to "deleted".
func markAllRecordsAsDeleted(db *sql.DB) error {
	updateQuery := `UPDATE products SET status = 'deleted'`
	_, err := db.Exec(updateQuery)
	return err
}

// fetchSpecification uses Chrome to fetch additional details from a URL.
func fetchSpecification(url string) (map[string]string, error) {
	ctx, cancel := chromedp.NewContext(context.Background())
	defer cancel()

	// Set a higher timeout
	ctx, cancel = context.WithTimeout(ctx, 20*time.Second)
	defer cancel()

	var specContent, category string
	err := chromedp.Run(ctx,
		chromedp.Navigate(url),
		chromedp.Text(`.react-tabs__tab-panel`, &specContent),
		chromedp.Text(`.breadcrumb-item:last-child`, &category),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch specification: %v", err)
	}

	data := map[string]string{
		"specification": specContent,
		"category":      category,
	}

	return data, nil
}

// initializeDB initializes the SQLite database with WAL mode and busy timeout.
func initializeDB() (*sql.DB, error) {
	db, err := sql.Open("sqlite3", fmt.Sprintf("file:%s?_busy_timeout=5000&_journal_mode=WAL", dbFileName))
	if err != nil {
		return nil, err
	}
	return db, nil
}

// executeWithRetry retries a database operation in case of SQLITE_BUSY or SQLITE_LOCKED errors.
func executeWithRetry(db *sql.DB, query string, args ...interface{}) error {
	var err error
	for i := 0; i < maxRetries; i++ {
		_, err = db.Exec(query, args...)
		if err == nil {
			return nil
		}
		if sqliteErr, ok := err.(sqlite3.Error); ok && (sqliteErr.Code == sqlite3.ErrBusy || sqliteErr.Code == sqlite3.ErrLocked) {
			time.Sleep(time.Duration(i+1) * time.Millisecond * 100) // Exponential backoff
			continue
		}
		return err
	}
	return fmt.Errorf("query failed after %d retries: %w", maxRetries, err)
}

// productExists checks if a product with the same unique code already exists in the database.
func productExists(db *sql.DB, uniqueCode string) (bool, float64, error) {
	var price float64
	query := `SELECT price FROM products WHERE unique_code = ? LIMIT 1`
	err := db.QueryRow(query, uniqueCode).Scan(&price)
	if err == sql.ErrNoRows {
		return false, 0, nil
	}
	if err != nil {
		return false, 0, err
	}
	return true, price, nil
}

// deleteFile sends a DELETE request to remove a document from a remote server
func deleteFile(documentID string) error {
	url := fmt.Sprintf("https://b2b.my-buddy.ai/v1/datasets/%s/documents/%s", datasetGUID, documentID)

	req, err := http.NewRequest("DELETE", url, nil)
	if err != nil {
		return fmt.Errorf("failed to create delete request: %v", err)
	}

	req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", authToken))

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("failed to execute delete request: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNoContent {
		fmt.Printf("Deleted document ID %s\n", documentID)
	} else {
		bodyBytes, _ := io.ReadAll(resp.Body)
		bodyString := string(bodyBytes)
		fmt.Printf("Failed to delete document ID %s: %d - %s\n", documentID, resp.StatusCode, bodyString)
	}

	return nil
}

// insertProduct inserts a product into the SQLite database with retry logic.
func insertProduct(db *sql.DB, product Product) error {
	dbMutex.Lock()
	defer dbMutex.Unlock()

	query := `INSERT INTO products (unique_code, price, mpn, status) VALUES (?, ?, ?, ?)`
	return executeWithRetry(db, query, product.UniqueCode, product.Price, product.MPN, product.Status)
}

// updateProductStatus updates a product's status in the database with retry logic.
func updateProductStatus(db *sql.DB, uniqueCode string, status string, price float64) error {
	dbMutex.Lock()
	defer dbMutex.Unlock()

	query := `UPDATE products SET status = ?, price = ? WHERE unique_code = ?`
	return executeWithRetry(db, query, status, price, uniqueCode)
}

// processItem processes a single XML item, extracts data, fetches specifications, and uploads the formatted file
func processItem(item Item) error {
	itemDict := make(map[string]string)
	var title, outputFilePath string

	itemDict["id"] = item.ID
	itemDict["price"] = fmt.Sprintf("%.2f", item.Price)
	itemDict["mpn"] = item.MPN

	title = item.ID
	outputFilePath = filepath.Join(folderPath, fmt.Sprintf("Prod_%s.txt", title))

	if fileExists(outputFilePath) {
		return nil
	}

	specData, err := fetchSpecification(item.Link)
	if err != nil {
		fmt.Printf("Failed to fetch specification for %s: %v\n", item.Link, err)
	} else {
		for key, value := range specData {
			itemDict[key] = value
		}
	}

	var formattedItem strings.Builder
	formattedItem.WriteString("[TITLE] ")
	formattedItem.WriteString(item.Title + "\n")
	formattedItem.WriteString("[Price] " + itemDict["price"] + "\n")
	if category, ok := itemDict["category"]; ok {
		formattedItem.WriteString("[Category] " + category + "\n")
	}
	formattedItem.WriteString("[BRAND] " + item.Brand + "\n")
	formattedItem.WriteString("\n[CONTENT] \n")

	formattedItem.WriteString("[DESCRIPTION] " + item.Description + "\n")
	formattedItem.WriteString("[LINK] " + item.Link + "\n")
	formattedItem.WriteString("[IMAGE LINK] " + item.ImageLink + "\n")
	formattedItem.WriteString("[AVAILABILITY] " + item.Availability + "\n")
	formattedItem.WriteString("[GTIN] " + item.GTIN + "\n")
	formattedItem.WriteString("[ID] " + item.ID + "\n")
	formattedItem.WriteString("[SKU] " + item.MPN + "\n")

	for key, value := range itemDict {
		if key != "id" && key != "price" && key != "mpn" && key != "category" {
			formattedItem.WriteString(fmt.Sprintf("[%s] %s\n", strings.Title(key), value))
		}
	}

	err = ioutil.WriteFile(outputFilePath, []byte(formattedItem.String()), 0644)
	if err != nil {
		fmt.Printf("Failed to write product file %s: %v\n", outputFilePath, err)
		return fmt.Errorf("Failed to write product file %s: %v\n", outputFilePath, err)
	}

	err = uploadFile(outputFilePath)
	if err != nil {
		fmt.Printf("Failed to upload product file %s: %v\n", outputFilePath, err)
		return fmt.Errorf("Failed to upload product file %s: %v\n", outputFilePath, err)
	}

	fmt.Printf("Processed and uploaded item with ID %s\n", title)
	return nil
}

// downloadXML downloads XML from a given URL with authentication and saves it to a file.
func downloadXML(url, username, password, outputPath string) error {
	client := &http.Client{
		Timeout: 10 * time.Second,
	}

	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return fmt.Errorf("failed to create HTTP request: %v", err)
	}

	req.SetBasicAuth(username, password)

	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("failed to download XML: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("bad response: %s", resp.Status)
	}

	outFile, err := os.Create(outputPath)
	if err != nil {
		return fmt.Errorf("failed to create output file: %v", err)
	}
	defer outFile.Close()

	_, err = io.Copy(outFile, resp.Body)
	if err != nil {
		return fmt.Errorf("failed to save XML to file: %v", err)
	}

	log.Printf("XML file downloaded successfully and saved to %s", outputPath)
	return nil
}

// Other unchanged methods...

func worker(db *sql.DB, wg *sync.WaitGroup, item Item, mutex *sync.Mutex) {
	defer wg.Done()

	mutex.Lock()
	defer mutex.Unlock()

	exists, price, err := productExists(db, item.ID)
	if err != nil {
		log.Printf("Error checking product existence: %v", err)
		return
	}

	if exists {
		if price == item.Price {
			err = updateProductStatus(db, item.ID, "existing", item.Price)
		} else {
			err = updateProductStatus(db, item.ID, "updated", item.Price)
			err = deleteFile(item.ID)
			if err != nil {
				log.Printf("Failed to delete document %s: %v", item.ID, err)
			}
			product := Product{
				UniqueCode: item.ID,
				Price:      item.Price,
				MPN:        item.MPN,
				Status:     "new",
			}
			err = insertProduct(db, product)
			if err == nil {
				err = processItem(item)
			}
		}
	} else {
		product := Product{
			UniqueCode: item.ID,
			Price:      item.Price,
			MPN:        item.MPN,
			Status:     "new",
		}
		err = insertProduct(db, product)
		if err == nil {
			err = processItem(item)
		}
	}

	if err != nil {
		log.Printf("Failed to process item: %v", err)
	}
}

func processXMLData(db *sql.DB, xmlPath string) error {
	xmlFile, err := os.Open(xmlPath)
	if err != nil {
		return fmt.Errorf("failed to open XML file: %v", err)
	}
	defer xmlFile.Close()

	byteValue, _ := ioutil.ReadAll(xmlFile)

	var rss RSS
	err = xml.Unmarshal(byteValue, &rss)
	if err != nil {
		return fmt.Errorf("failed to unmarshal XML: %v", err)
	}

	var wg sync.WaitGroup
	mutex := &sync.Mutex{}
	sem := make(chan struct{}, maxWorkers)

	for _, item := range rss.Channel.Items {
		sem <- struct{}{}
		wg.Add(1)
		go func(item Item) {
			defer func() { <-sem }()
			worker(db, &wg, item, mutex)
		}(item)
	}

	wg.Wait()

	return nil
}

func main() {
	url := "restapiurl"
	username := "usenrmae"
	password := "password"
	outputPath := "./facebook_shop.xml"

	err := downloadXML(url, username, password, outputPath)
	if err != nil {
		log.Fatalf("Error: %v", err)
	}

	db, err := initializeDB()
	if err != nil {
		log.Fatalf("Failed to initialize the database: %v\n", err)
	}
	defer db.Close()

	err = markAllRecordsAsDeleted(db)
	if err != nil {
		log.Fatalf("Failed to mark records as deleted: %v\n", err)
	}

	err = processXMLData(db, outputPath)
	if err != nil {
		log.Fatalf("Failed to process XML data: %v\n", err)
	}

	fmt.Println("Database update complete.")
}
