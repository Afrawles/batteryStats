package main

import (
	"encoding/json"
	"io"
	"io/fs"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/parquet-go/parquet-go"
)

type BranchReading struct {
	CurrentA float32 //Current_Actual_P1/P2/P3 [A]
	VoltageV float32 //Voltage_Actual_P1/P2/P3 [V]
}

type Reading struct {
	Cycle     string
	SemiCycle string
	Timestamp time.Time

	CurrentA float32 //Current_Actual_Battery [A]
	VoltageV float32 //Voltage_Actual_Battery [V]

	SocPercent float32 //SoC_Actual_Battery [percent]

	VoltageAvgCell float32
	VoltageMinCell float32
	VoltageMaxCell float32

	Branches [3]BranchReading
}

type Record struct {
	Cycle     string    `parquet:"Cycle"`
	SemiCycle string    `parquet:"Semicycle"`
	Timestamp time.Time `parquet:"Timestamp"`

	CurrentA float32 `parquet:"Current_Actual_Battery [A]"` //Current_Actual_Battery [A]
	VoltageV float32 `parquet:"Voltage_Actual_Battery [V]"` //Voltage_Actual_Battery [V]

	SocPercent float32 `parquet:"SoC_Actual_Battery [percent]"` //SoC_Actual_Battery [percent]

	VoltageAvgCell float32 `parquet:"Voltage_Avg_Cell [V]"`
	VoltageMinCell float32 `parquet:"Voltage_Min_Cell [V]"`
	VoltageMaxCell float32 `parquet:"Voltage_Max_Cell [V]"`

	CurrentP1A float32 `parquet:"Current_Actual_P1 [A]"` //Current_Actual_P1/P2/P3 [A]
	CurrentP2A float32 `parquet:"Current_Actual_P2 [A]"` //Current_Actual_P1/P2/P3 [A]
	CurrentP3A float32 `parquet:"Current_Actual_P3 [A]"` //Current_Actual_P1/P2/P3 [A]
	VoltageP1V float32 `parquet:"Voltage_Actual_P1 [V]"` //Voltage_Actual_P1/P2/P3 [V]
	VoltageP2V float32 `parquet:"Voltage_Actual_P2 [V]"` //Voltage_Actual_P1/P2/P3 [V]
	VoltageP3V float32 `parquet:"Voltage_Actual_P3 [V]"` //Voltage_Actual_P1/P2/P3 [V]
}

func main() {
	var dataFiles []string

	err := filepath.WalkDir("data", func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}

		if strings.HasSuffix(path, ".parquet") {
			dataFiles = append(dataFiles, path)
		}

		return nil
	})

	if err != nil {
		log.Fatal(err)
	}

	nc, err := nats.Connect(nats.DefaultURL)

	if err != nil {
		log.Fatal(err)
	}

	for i, v := range dataFiles {
		f, err := os.OpenFile(v, os.O_RDONLY, 0o400)

		if err != nil {
			log.Fatal(err)
		}
		defer f.Close()

		reader := parquet.NewGenericReader[Record](f)
		defer reader.Close()

		b := make([]Record, 1000)

		for {
			n, err := reader.Read(b)

			if n == 0 || err == io.EOF {
				break
			}

			if err != nil {
				log.Fatal(err)
			}

			for j := 0; j < n; j++ {
				row := b[i]
				record := Reading{
					Cycle:          row.Cycle,
					SemiCycle:      row.SemiCycle,
					Timestamp:      row.Timestamp,
					CurrentA:       row.CurrentA,
					VoltageV:       row.VoltageV,
					SocPercent:     row.SocPercent,
					VoltageAvgCell: row.VoltageAvgCell,
					VoltageMinCell: row.VoltageMinCell,
					VoltageMaxCell: row.VoltageMaxCell,
					Branches: [3]BranchReading{
						{
							CurrentA: row.CurrentP1A,
							VoltageV: row.VoltageP1V,
						},
						{
							CurrentA: row.CurrentP2A,
							VoltageV: row.VoltageP2V,
						},
						{
							CurrentA: row.CurrentP3A,
							VoltageV: row.VoltageP3V,
						},
					},
				}

				payload, err := json.Marshal(record)
				if err != nil {
					continue
				}

				err = nc.Publish("bms.reading", payload)
				if err != nil {
					log.Fatal(err)
				}

			}

		}
	}

}

