package main

import (
	"bufio"
	"log"
	"os"
	"strconv"
	"strings"

	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

const FILE_NAME string = "worldcitiespop.txt"

/**
Geo
*/
type Geo struct {
	Country    string  `json:"country" bson:"country"`
	City       string  `json:"city" bson:"city"`
	AccentCity string  `json:"city_acc" bson:"city_acc"`
	Region     string  `json:"state" bson:"state"`
	Latitude   float64 `json:"latitude" bson:"latitude"`
	Longitude  float64 `json:"longitude" bson:"longitude"`
}

/**
MongoDb Session
*/
func Session() *mgo.Session {
	s, err := mgo.Dial("mongodb://localhost")

	if err != nil {
		panic(err)
	}

	s.SetMode(mgo.Monotonic, true)

	return s
}

func main() {
	// Open specified file
	f, err := os.Open(FILE_NAME)

	if err != nil {
		log.Fatalln(err)
	}

	// close file
	defer f.Close()

	//init bufio
	bs := bufio.NewScanner(f)

	// init cities
	var cities []Geo

	/**
	Scan through file
	*/
	for bs.Scan() {
		line := bs.Text()

		x := strings.Split(line, ",")

		lat := strings.Join(x[5:6], "")
		latf, _ := strconv.ParseFloat(lat, 64)

		lng := strings.Join(x[6:7], "")
		lngf, _ := strconv.ParseFloat(lng, 64)

		country := strings.ToUpper(strings.Join(x[0:1], ""))
		_state := strings.ToUpper(strings.Join(x[3:4], ""))

		var state string = _state

		/**
		For Canada
		Convert Province code to
		Province ISO
		*/
		if country == "CA" {
			state = GetState(_state)
		}

		city := Geo{
			Country:    country,
			City:       string(strings.Join(x[1:2], "")),
			AccentCity: string(strings.Join(x[2:3], "")),
			Region:     state,
			Latitude:   latf,
			Longitude:  lngf,
		}

		/**
		Append only for
		US and Canada
		*/
		if country == "US" || country == "CA" {
			cities = append(cities, city)
		}

	}

	// Bulk insert
	Db(cities)
}

/**
Bulk insert into mongodb

@param []Geo g
@return void
*/
func Db(g []Geo) {
	s := Session()
	c := s.DB("events").C("cities")

	// init bulk insert
	x := c.Bulk()
	x.Unordered()

	// Range through all the cities
	for _, r := range g {
		// query
		query := bson.M{
			"country":   r.Country,
			"city":      r.City,
			"city_acc":  r.AccentCity,
			"state":     r.Region,
			"latitude":  r.Latitude,
			"longitude": r.Longitude,
		}

		// insert
		x.Insert(query)
	}

	//execute bulk insert
	x.Run()

	//close mongodb session
	defer s.Close()
}

/**
Convert Province code to
Province ISO code

@param string code
@return string ret
*/
func GetState(code string) (ret string) {
	switch code {
	case "01":
		ret = "AB"
	case "02":
		ret = "BC"
	case "03":
		ret = "MB"
	case "04":
		ret = "NB"
	case "05":
		ret = "NL"
	case "06":
		ret = ""
	case "07":
		ret = "NS"
	case "08":
		ret = "ON"
	case "09":
		ret = "PE"
	case "10":
		ret = "QC"
	case "11":
		ret = "SK"
	case "12":
		ret = "YT"
	case "13":
		ret = "NT"
	case "14":
		ret = "NU"
	}

	return ret
}
