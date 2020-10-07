package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"regexp"
	"strconv"
	"strings"

	"github.com/PuerkitoBio/goquery"
	"io/ioutil"

	"github.com/valyala/fasthttp"
	"time"

	"github.com/jinzhu/gorm"
	_ "github.com/jinzhu/gorm/dialects/mysql"
	"sync"
)

/*
author : sohai
date: 07-oct-2020
*/

type DB struct {
	DbMysql *gorm.DB
}

var (
	onceDbMysql     sync.Once
	instanceDBMysql *DB
)

type Config struct {
	Database struct {
		Mysql struct {
			Name     string `json:"name"`
			Host     string `json:"host"`
			Port     int    `json:"port"`
			Username string `json:"username"`
			Password string `json:"password"`
			LogMode  bool   `json:"logMode"`
		} `json:"mysql"`
	}
}

type Disclosure struct {
	ID                          uint64     `gorm:"primary_key"`
	Code                        string     `sql:"type:text;" gorm:"column:code"`
	Company                     string     `sql:"type:text;" gorm:"column:company"`
	MarketCapitalization        int64      `sql:"type:int;" gorm:"column:market_capitalization"`
	DisclosureDate              time.Time  `gorm:"column:disclosure_date"`
	FourthQuarter               string     `sql:"type:text;" gorm:"column:fourth_quarter"`
	Sales                       int64      `sql:"type:int;" gorm:"column:sales"`
	OperatingProfit             int64      `sql:"type:int;" gorm:"column:operating_profit"`
	OrdinaryProfit              int64      `sql:"type:int;" gorm:"column:ordinary_profit"`
	NetProfit                   int64      `sql:"type:int;" gorm:"column:net_profit"`
	EPS                         float64    `sql:"type:float;" gorm:"column:EPS"`
	YOYSales                    float64    `sql:"type:float;" gorm:"column:YOY_sales"`
	YOYOperatingProfit          float64    `sql:"type:float;" gorm:"column:YOY_operating_profit"`
	YOYOrdinaryProfit           float64    `sql:"type:float;" gorm:"column:YOY_ordinary_profit"`
	YOYNetProfit                float64    `sql:"type:float;" gorm:"column:YOY_net_profit"`
	ClosePriceOneBusinessDayAgo float64    `sql:"type:float;" gorm:"column:close_price_one_business_day_ago"`
	CreatedAt                   *time.Time `gorm:"column:create_time"`
	UpdatedAt                   *time.Time `gorm:"column:updated_at"`
	DeletedAt                   *time.Time `gorm:"column:deleted_at" sql:"index"`
}

// This connection for L4 application database (read only)
func GetInstanceMysqlDb() *gorm.DB {
	file, err := ioutil.ReadFile("./config.json")
	if err != nil {
		log.Fatalln(err)
	}

	Config := Config{}
	json.Unmarshal([]byte(file), &Config)

	onceDbMysql.Do(func() {
		mysqlInfo := Config.Database.Mysql
		logs := fmt.Sprintf("[INFO] Connected to MYSQL TYPE = %s | LogMode = %+v", mysqlInfo.Host, mysqlInfo.LogMode)

		dbConfig := mysqlInfo.Username + ":" + mysqlInfo.Password + "@tcp(" + fmt.Sprintf("%s:%d", mysqlInfo.Host, +mysqlInfo.Port) + ")/" + mysqlInfo.Name
		mysqlConnect := dbConfig + "?charset=utf8&parseTime=True&loc=Local"
		dbConnection, err := gorm.Open("mysql", mysqlConnect)
		if err != nil {
			logs = "[ERROR] Failed to connect to MYSQL. Config=" + mysqlInfo.Host + "| " + err.Error()
			os.Exit(1)
		}
		fmt.Println(logs)
		instanceDBMysql = &DB{DbMysql: dbConnection}
		dbConnection.LogMode(mysqlInfo.LogMode)
		dbConnection.SingularTable(true)
		dbConnection.DB().SetMaxIdleConns(10)
		dbConnection.DB().SetMaxOpenConns(20)
		dbConnection.DB().SetConnMaxLifetime(10 * time.Minute)
	})
	return instanceDBMysql.DbMysql
}

func loadFile(filename string) string {
	content, _ := ioutil.ReadFile(filename)
	output := string(content)
	return output
}

func SearchEngine(url string) (output string) {
	req := fasthttp.AcquireRequest()
	defer fasthttp.ReleaseRequest(req)

	req.SetRequestURI(url)
	req.Header.SetMethod("GET")
	req.Header.Set("user-agent", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/85.0.4183.121 Safari/537.36")
	req.Header.Set("cache-control", "no-cache")
	req.Header.Set("accept-language", "en-US,en;q=0.9,id;q=0.8")

	resp := fasthttp.AcquireResponse()
	defer fasthttp.ReleaseResponse(resp)

	client := &fasthttp.Client{}
	if err := client.DoTimeout(req, resp, 15*time.Second); err != nil {
		fmt.Printf("err: %+v\n", err)
	}

	body := resp.Body()
	output = string(body)
	return
}

func parseDataTable(response, typeFilter string) (rows [][]string) {
	var headings, row []string
	// var rows [][]string

	doc, err := goquery.NewDocumentFromReader(strings.NewReader(string(response)))
	if err != nil {
		fmt.Println("No url found")
	}

	// Find each table
	doc.Find("table").Each(func(index int, tablehtml *goquery.Selection) {
		temp_row := ""
		tablehtml.Find("tr").Each(func(indextr int, rowhtml *goquery.Selection) {
			rowhtml.Find("th").Each(func(indexth int, tableheading *goquery.Selection) {

				if typeFilter == "tyn-imarket.com" && index == 1 {
					headings = append(headings, tableheading.Text())
				}
				if typeFilter != "tyn-imarket.com" {
					headings = append(headings, tableheading.Text())
				}
			})

			rowhtml.Find("td").Each(func(indexth int, tablecell *goquery.Selection) {
				if typeFilter == "tyn-imarket.com" && index == 1 {
					row = append(row, tablecell.Text())
				}
				if typeFilter != "tyn-imarket.com" {
					row = append(row, tablecell.Text())
				}
			})

			if typeFilter == "tyn-imarket.com" && index == 1 {
				if len(row) > 0 {
					if len(row[0]) > 0 {
						temp_row = row[0]
					}

					if len(row[0]) == 0 {
						row[0] = temp_row
					}
					rows = append(rows, row)
					row = nil
				}
			}
			if typeFilter != "tyn-imarket.com" {
				rows = append(rows, row)
				row = nil
			}
		})

	})
	// fmt.Println("####### headings = ", len(headings), headings)
	// fmt.Println("####### rows = ", len(rows), rows)

	return
}


func (Disclosure) TableName() string {
	return "disclosure"
}

func main() {
	GetInstanceMysqlDb().AutoMigrate(&Disclosure{})

	page := 1
	company_number := 1
	for {
		urlPage := fmt.Sprintf("https://info.finance.yahoo.co.jp/ranking/?kd=4&tm=d&vl=a&mk=1&p=%d", page)
		responseInfoFinanceYahoo := SearchEngine(urlPage)
		if !strings.Contains(responseInfoFinanceYahoo, "次へ") {
			fmt.Print("don't have pagination\n")
			break
		}

		// current_time := time.Now()
		rows_info_finance := parseDataTable(responseInfoFinanceYahoo, "info.finance.yahoo.co.jp")
		for _, row_info_finance := range rows_info_finance {
			if len(row_info_finance) == 10 {

				company_code := row_info_finance[1]
				responseTYN := SearchEngine("https://tyn-imarket.com/stocks/" + company_code)
				rows_info_tyn := parseDataTable(responseTYN, "tyn-imarket.com")

				for _, rows_info_tyn := range rows_info_tyn {
					disclosure := Disclosure{}
					disclosure.Code = company_code
					disclosure.Company = row_info_finance[3]
					fmt.Printf("PAGE : %d/%d | company_code : %s | company_name: %s\n", page, company_number, company_code, disclosure.Company)

					marketCapitalization, _ := strconv.ParseInt(strings.Replace(row_info_finance[7], ",", "", -1), 10, 64)
					disclosure.MarketCapitalization = marketCapitalization

					if len(rows_info_tyn) == 13 {
						disclosure.FourthQuarter = strings.Replace(rows_info_tyn[1], "予想", "", -1)

						sales, _ := strconv.ParseInt(strings.Replace(rows_info_tyn[2], ",", "", -1), 10, 64)
						disclosure.Sales = sales
						operatingProfit, _ := strconv.ParseInt(strings.Replace(rows_info_tyn[3], ",", "", -1), 10, 64)

						disclosure.OperatingProfit = operatingProfit
						ordinaryProfit, _ := strconv.ParseInt(strings.Replace(rows_info_tyn[4], ",", "", -1), 10, 64)
						disclosure.OrdinaryProfit = ordinaryProfit

						netProfit, _ := strconv.ParseInt(strings.Replace(rows_info_tyn[5], ",", "", -1), 10, 64)
						disclosure.NetProfit = netProfit

						eps, _ := strconv.ParseFloat(strings.Replace(rows_info_tyn[6], ",", "", -1), 64)
						disclosure.EPS = eps

						yoySales, _ := strconv.ParseFloat(strings.Replace(rows_info_tyn[9], ",", "", -1), 64)
						disclosure.YOYSales = yoySales

						YOYOperatingProfit, _ := strconv.ParseFloat(strings.Replace(rows_info_tyn[10], ",", "", -1), 64)
						disclosure.YOYOperatingProfit = YOYOperatingProfit

						YOYOrdinaryProfit, _ := strconv.ParseFloat(strings.Replace(rows_info_tyn[11], ",", "", -1), 64)
						disclosure.YOYOrdinaryProfit = YOYOrdinaryProfit

						YOYNetProfit, _ := strconv.ParseFloat(strings.Replace(rows_info_tyn[12], ",", "", -1), 64)
						disclosure.YOYNetProfit = YOYNetProfit

						regex_str := `<td class="text-center">` + rows_info_tyn[0] + `</td><td class="text-center"><a href='(.*?)' target='_blank'>` + disclosure.FourthQuarter
						attrName := regexp.MustCompile(regex_str).FindAllStringSubmatch(responseTYN, -1)

						pdfFileName := ""
						pdfFileNameTemp := strings.Split(attrName[0][1], ".pdf")
						if len(pdfFileNameTemp) == 2 {
							pdfFileName = pdfFileNameTemp[0]
						} else if len(pdfFileNameTemp) > 2 {
							pdfFileName = strings.Split(pdfFileNameTemp[1], "<a href='")[1]
						}

						// year_current := current_time.Format("2006")
						// if company_code == "6098" {
						// if ("4Q" == disclosure.FourthQuarter || "3Q" == disclosure.FourthQuarter)  && strings.Contains(pdfFileName, year_current){
						temp_file_name := strings.Split(pdfFileName, "/")
						fileYear, fileMonth, fileDay := temp_file_name[2], temp_file_name[3], temp_file_name[4]

						disclosureDate, _ := time.Parse("2006-1-2", fmt.Sprintf("%s-%s-%s", fileYear, fileMonth, fileDay))
						disclosure.DisclosureDate = disclosureDate

						urlInfo := "https://info.finance.yahoo.co.jp/history/?code=" + disclosure.Code + ".T&sy=" + fileYear + "&sm=" + fileMonth + "&sd=" + fileDay + "&ey=" + fileYear + "&em=" + fileMonth + "&ed=" + fileDay + "&tm=d"
						// fmt.Println(urlInfo)
						responseInfoFinanceYahoo := SearchEngine(urlInfo)
						rows_info_tyn_history_details := parseDataTable(responseInfoFinanceYahoo, "info.finance.yahoo.co.jp")
						for _, rows_info_tyn_history_detail := range rows_info_tyn_history_details {
							if len(rows_info_tyn_history_detail) == 7 {
								closePriceOneBusinessDayAgo, _ := strconv.ParseFloat(strings.Replace(rows_info_tyn_history_detail[4], ",", "", -1), 64)
								disclosure.ClosePriceOneBusinessDayAgo = closePriceOneBusinessDayAgo
								break
							}
						}
						disclosure_check := Disclosure{}
						GetInstanceMysqlDb().Table("disclosure").Where("code = ?", disclosure.Code).Where("fourth_quarter = ? ", disclosure.FourthQuarter).Where("disclosure_date = ?", disclosureDate).First(&disclosure_check)
						if disclosure_check.ID == 0 {
							if err := GetInstanceMysqlDb().Table("disclosure").Create(&disclosure).Error; err != nil {
								fmt.Printf("err while insert disclosure: %+v\n", err)
							}
						}
						// }
					}
				}
				// break

				company_number += 1
			}
		}

		page += 1
	}
	// responseTYN := loadFile("sample/sample_response_tyn-imarket.html")
	// responseInfoFinanceYahoo := loadFile("sample/sample_response_info_finance_yahoo.html")
	// responseStockFinanceYahoo := loadFile("sample/sample_response_stocks_finance_yahoo.html")
	// responseTYN := SearchEngine("https://tyn-imarket.com/stocks/6096")
	// responseInfoFinanceYahoo := SearchEngine("https://info.finance.yahoo.co.jp/ranking/?kd=4&tm=d&vl=a&mk=1&p=1")
	// responseStockFinanceYahoo := SearchEngine("https://stocks.finance.yahoo.co.jp/stocks/history/?code=4565.T")

	// parseDataTable(responseTYN, "tyn-imarket.com")
	// parseDataTable(responseInfoFinanceYahoo, "info.finance.yahoo.co.jp")
	// parseDataTable(responseStockFinanceYahoo, "stocks.finance.yahoo.co.jp")

}
