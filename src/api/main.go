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
	Case_Num             string  `json:"Case_Num"`
	Veh_Num              int     `json:"VEH_Num"`
	Occupation_Num       int     `json:"Occupation_Num"`
	Hit_Run              int     `json:"Hit_Run"`
	VIN                  string  `json:"VIN"`
	Model_Year           int     `json:"Model_Year"`
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
	// http.HandleFunc("/api/vehicles", func(w http.ResponseWriter, r *http.Request) {
	// 	fmt.Fprintf(w, "The current time is: %s", time.Now())
	// })
	http.ListenAndServe(":8080", router)
}

func getCount(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	var count int
	result, err := db.Query("SELECT COUNT(*) FROM Vehicles")
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
		a.Case_Num, a.VEH_Num, v.CASENUM, v.VEH_NO, COALESCE(a.Occupation_Num, -1) as Occupation_Num,
		COALESCE(a.Hit_Run, -1) as Hit_Run, a.VIN, COALESCE(a.Model_Year, -1) as Model_Year, 
		COALESCE(v.VehicleType, "") as VehicleType,
		COALESCE(v.ManufacturerFullName, "") as ManufacturerFullName,
		COALESCE(v.Make, "") as Make,
		COALESCE(v.Model, "") as Model, COALESCE(v.PlantCountry, "") as PlantCountry, 
		COALESCE(v.PlantState, "") as PlantState, COALESCE(v.PlantCity, "") as PlantCity,
		COALESCE(v.PlantCompanyName, "") as PlantCompanyName, 
		COALESCE(v.BasePrice, -1) as BasePrice
		FROM Vehicles as a LEFT JOIN vpicdecode as v 
		ON a.Case_Num=v.CASENUM 
		AND a.VEH_Num=v.VEH_NO LIMIT ? OFFSET ?`, pageSize, offset)
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
