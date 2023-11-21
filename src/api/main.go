package main

import (
	"database/sql"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"strconv"

	"github.com/gorilla/mux"

	_ "github.com/go-sql-driver/mysql"
	"github.com/joho/godotenv"
)

type Vehicle struct {
	Case_Num             string  `json:"CASENUM"`
	Veh_Num              int     `json:"VEH_NO"`
	Occupation_Num       int     `json:"NUMOCCS"`
	Hit_Run              int     `json:"HIT_RUN"`
	VIN                  string  `json:"VIN"`
	Model_Year           int     `json:"MDLYR_IM"`
	Make                 string  `json:"Make"`
	Model                string  `json:"Model"`
	VehicleType          string  `json:"VehicleType"`
	ManufacturerFullName string  `json:"ManufacturerFullName"`
	PlantCountry         string  `json:"PlantCountry"`
	PlantState           string  `json:"PlantState"`
	PlantCity            string  `json:"PlantCity"`
	PlantCompanyName     string  `json:"PlantCompanyName"`
	BasePrice            float64 `json:"BasePrice"`
}

type Frequency struct {
	Make  string `json:"Make"`
	Model string `json:"Model"`
	Count int    `json:"Count"`
}

var IgnoreColumn ignoreColumn

type ignoreColumn struct{}

func (ignoreColumn) Scan(value interface{}) error {
	return nil
}

var db *sql.DB
var err error

func main() {
	// Load environment variables from file.
	if err := godotenv.Load(); err != nil {
		log.Fatalf("failed to load environment variables: %v", err)
	}
	db, err = sql.Open("mysql", os.Getenv("DSN"))
	if err != nil {
		log.Fatalf("failed to connect: %v", err)
	}
	defer db.Close()

	router := mux.NewRouter()

	router.HandleFunc("/api/vehicles", getVehicles).Methods("GET")
	router.HandleFunc("/api/count", getCount).Methods("GET")
	router.HandleFunc("/api/frequency", getVehicleFrequency).Methods("GET")
	// http.HandleFunc("/api/vehicles", func(w http.ResponseWriter, r *http.Request) {
	// 	fmt.Fprintf(w, "The current time is: %s", time.Now())
	// })
	http.ListenAndServe(":8080", router)
}

// SELECT f.Make, SUM(Count)/t.Total * 100 as percentage FROM frequency as f CROSS JOIN (SELECT SUM(Count) as Total FROM frequency) AS t GROUP BY f.Make, t.Total ORDER BY percentage DESC;

func getVehicleFrequency(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	page := r.URL.Query().Get("page")
	size := r.URL.Query().Get("size")
	searchTerm := r.URL.Query().Get("searchTerm")
	pageNum, err := strconv.Atoi(page)
	if err != nil {
		pageNum = 1
	}
	pageSize, err := strconv.Atoi(size)
	if err != nil || pageSize > 100 {
		pageSize = 10
	}
	offset := (pageNum - 1) * pageSize
	var frequencies []Frequency
	var result *sql.Rows
	if searchTerm != "" {
		result, err = db.Query(`SELECT * FROM frequency WHERE CONCAT(Make, Model) RLIKE ? ORDER BY Count DESC LIMIT ? OFFSET ?`, searchTerm, pageSize, offset)
	} else {
		result, err = db.Query(`SELECT * FROM frequency ORDER BY Count DESC LIMIT ? OFFSET ?`, pageSize, offset)
	}
	if err != nil {
		panic(err.Error())
	}

	defer result.Close()

	for result.Next() {
		var frequency Frequency
		err := result.Scan(&frequency.Make, &frequency.Model, &frequency.Count)
		if err != nil {
			panic(err.Error())
		}
		frequencies = append(frequencies, frequency)
	}
	json.NewEncoder(w).Encode(frequencies)
}

func getCount(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	var count int
	result, err := db.Query("SELECT COUNT(*) FROM vehicle")
	if err != nil {
		panic(err.Error())
	}
	defer result.Close()

	for result.Next() {
		err := result.Scan(&count)
		if err != nil {
			panic(err.Error())
		}
	}
	json.NewEncoder(w).Encode(count)
}

func getVehicles(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	page := r.URL.Query().Get("page")
	size := r.URL.Query().Get("size")
	pageNum, err := strconv.Atoi(page)
	if err != nil {
		pageNum = 1
	}
	pageSize, err := strconv.Atoi(size)
	if err != nil || pageSize > 100 {
		pageSize = 10
	}
	var vehicles []Vehicle

	offset := (pageNum - 1) * pageSize
	result, err := db.Query(`SELECT
		a.CASENUM, a.VEH_NO, v.CASENUM, v.VEH_NO, COALESCE(a.NUMOCCS, -1) as NUMOCCS,
		COALESCE(a.HIT_RUN, -1) as HIT_RUN, a.VIN, COALESCE(a.MDLYR_IM, -1) as MDLYR_IM, 
		COALESCE(v.VehicleType, "") as VehicleType,
		COALESCE(v.ManufacturerFullName, "") as ManufacturerFullName,
		COALESCE(v.Make, "") as Make,
		COALESCE(v.Model, "") as Model, COALESCE(v.PlantCountry, "") as PlantCountry, 
		COALESCE(v.PlantState, "") as PlantState, COALESCE(v.PlantCity, "") as PlantCity,
		COALESCE(v.PlantCompanyName, "") as PlantCompanyName, 
		COALESCE(v.BasePrice, -1) as BasePrice
		FROM vehicle as a LEFT JOIN vpicdecode as v 
		ON a.CASENUM=v.CASENUM 
		AND a.VEH_NO=v.VEH_NO LIMIT ? OFFSET ?`, pageSize, offset)
	if err != nil {
		panic(err.Error())
	}

	defer result.Close()

	for result.Next() {
		var vehicle Vehicle
		err := result.Scan(&vehicle.Case_Num, &vehicle.Veh_Num,
			IgnoreColumn, IgnoreColumn,
			&vehicle.Occupation_Num, &vehicle.Hit_Run,
			&vehicle.VIN, &vehicle.Model_Year, &vehicle.VehicleType,
			&vehicle.ManufacturerFullName, &vehicle.Make, &vehicle.Model,
			&vehicle.PlantCountry,
			&vehicle.PlantState, &vehicle.PlantCity,
			&vehicle.PlantCompanyName, &vehicle.BasePrice)
		if err != nil {
			panic(err.Error())
		}
		vehicles = append(vehicles, vehicle)
	}

	json.NewEncoder(w).Encode(vehicles)
}
