package main

import (
	"fmt"
	"io"
	"io/fs"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/parquet-go/parquet-go"
)

type BranchReading struct {
	CurrentA float64 //Current_Actual_P1/P2/P3 [A]
	VoltageV float64 //Voltage_Actual_P1/P2/P3 [V]
}

type Reading struct {
	Cycle int64
	SemiCycle int64
	Timestamp time.Time

	CurrentA float64 //Current_Actual_Battery [A]
	VoltageV float64 //Voltage_Actual_Battery [V]

	SocPercent float64 //SoC_Actual_Battery [percent]

	VoltageAvgCell float64
	VoltageMinCell float64
	VoltageMaxCell float64

	Branches [3]BranchReading

}

type Record struct {
	Cycle string `parquet:"Cycle"`
	SemiCycle string `parquet:"SemiCycle"`
	Timestamp time.Time `parquet:"Timestamp"`

	CurrentA float32 `parquet:"Current_Actual_Battery [A]"`//Current_Actual_Battery [A]
	VoltageV float32 `parquet:"Voltage_Actual_Battery [V]"`//Voltage_Actual_Battery [V]

	SocPercent float32 `parquet:"SoC_Actual_Battery [percent]"` //SoC_Actual_Battery [percent]

	VoltageAvgCell float32 `parquet:"Voltage_Avg_Cell [V]"`
	VoltageMinCell float32 `parquet:"Voltage_Min_Cell [V]"`
	VoltageMaxCell float32 `parquet:"Voltage_Max_Cell [V]"`
	
	CurrentP1A float32 `parquet:"Current_Actual_P1 [A]"`//Current_Actual_P1/P2/P3 [A]
	CurrentP2A float32 `parquet:"Current_Actual_P2 [A]"`//Current_Actual_P1/P2/P3 [A]
	CurrentP3A float32 `parquet:"Current_Actual_P3 [A]"` //Current_Actual_P1/P2/P3 [A]
	VoltageP1V float32 `parquet:"Voltage_Actual_P1 [V]"`//Voltage_Actual_P1/P2/P3 [V]
	VoltageP2V float32 `parquet:"Voltage_Actual_P2 [V]"`//Voltage_Actual_P1/P2/P3 [V]
	VoltageP3V float32 `parquet:"Voltage_Actual_P3 [V]"`//Voltage_Actual_P1/P2/P3 [V]
}

func main() {
	var dataFiles []string

	filepath.WalkDir("data", func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}

		if strings.HasSuffix(path, ".parquet") {
			dataFiles = append(dataFiles, path)
		} 

		return nil
	})

	f, _ := os.OpenFile(dataFiles[0], os.O_RDONLY, 0400)
	defer f.Close()

	reader := parquet.NewGenericReader[Record](f)
	defer reader.Close()

	b := make([]Record, 10)

	for {
		n, err := reader.Read(b)

		if n == 0 || err == io.EOF {
			break
		}

		if err != nil {
			log.Fatal(err)
		}

		for i := 0; i < n; i ++ {
			row := b[i]

			fmt.Println("*********")
			fmt.Println(row)
			fmt.Println("*********")

			break
		}

		break
	}
	
}
