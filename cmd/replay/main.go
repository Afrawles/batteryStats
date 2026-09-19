package main

import (
	// "encoding/json"
	"fmt"
	"io"
	"io/fs"
	"log"
	"math"
	"os"
	"path/filepath"
	"strings"
	"time"

	// "github.com/nats-io/nats.go"
	"github.com/parquet-go/parquet-go"
)

const (
	// paper used NCR18650B panasonic
	// Rateg capacity 3200mAh
	ratedCapacity      float32 = 9.6  //Ah for the 3 parallel branches
	imbalanceThreshold         = 0.05 // 5% off the ideal 1/3
	packID                     = "default"
	constantDischarge          = "Capacity check Discharge"
	constantCharge             = "Capacity check Charge"
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

	// nc, err := nats.Connect(nats.DefaultURL)

	// if err != nil {
	// 	log.Fatal(err)
	// }

	var resistanceHistory = map[string][]float32{}
	cycleCount := 0

	for _, v := range dataFiles {
		f, err := os.OpenFile(v, os.O_RDONLY, 0o400)

		if err != nil {
			fmt.Println(err)
			continue
		}
		defer f.Close()

		reader := parquet.NewGenericReader[Record](f)
		defer reader.Close()

		b := make([]Record, 1000)

		var (
			isDischarge, isCharge bool
			dQN                   float64 // rolling sum of the previous passed charge
			cycleRows             = []Reading{}
			resistance            float32
			rFlag                 bool
			iFlag                 bool
			conf                  string
			previousTimestamp     time.Time
		)

		for {
			n, err := reader.Read(b)

			if n > 0 {

				for j := 0; j < n; j++ {

					row := b[j]

					// Application Note Battery Gauging Algorithm Comparison
					// Texa Instruments by Nick Richards paper
					//dQN = (ElapsedTimeN + 1 − ElapsedTimeN × Current) / 3600 + dQN − 1

					if row.SemiCycle == constantDischarge {

						if !previousTimestamp.IsZero() {
							elapsedTime := row.Timestamp.Sub(previousTimestamp).Seconds()
							dQN += (elapsedTime * float64(row.CurrentA)) / 3600
						}

						previousTimestamp = row.Timestamp

					}

					if row.SemiCycle == constantDischarge {
						isDischarge = true
					}
					if row.SemiCycle == constantCharge {
						isCharge = true
					}

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

					cycleRows = append(cycleRows, record)

					// payload, err := json.Marshal(record)
					// if err != nil {
					// 	continue
					// }
					//
					// err = nc.Publish("bms.reading", payload)
					// if err != nil {
					// 	log.Fatal(err)
					// }

				}

			}

			if err == io.EOF {
				break
			}

			if err != nil {
				fmt.Println(err)
				continue
			}

		}

		if !isCharge || !isDischarge {
			continue
		}

		resistance, ok := calcInternalResistance(cycleRows)
		if !ok {
			continue
		}

		rFlag = resistanceFlag(resistance, resistanceHistory[packID])
		resistanceHistory[packID] = append(resistanceHistory[packID], resistance)

		iFlag = calcCycleImbalance(cycleRows)

		soh := dQN / float64(ratedCapacity)

		conf = confidenceLevel(cycleCount)

		cycleCount++

		// TODO: save result to innxlufdb
		fmt.Println(resistance)
		fmt.Println(rFlag)
		fmt.Println(iFlag)
		fmt.Println(conf)
		fmt.Println(soh)

	}

}

func calcInternalResistance(rows []Reading) (float32, bool) {
	//InternalResistance = OCV(DoD, T) − PresentLoadedVoltage / MeasuredCurrent
	ocv := findLastChargeVoltage(rows) // simplied ocv
	presentLoadedVoltage, meassuredCurrent := findFirstDischargeReading(rows)

	if meassuredCurrent <= 0 {
		return 0.0, false
	}

	return (ocv - presentLoadedVoltage) / meassuredCurrent, true
}

// flag increase of resitance by 20%
func resistanceFlag(latest float32, history []float32) bool {
	if len(history) == 0 {
		return false
	}

	var total float32

	for _, v := range history {
		total += v
	}

	if total <= 0 {
		return false
	}

	baseline := total / float32(len(history))
	increase := (latest - baseline) / baseline

	return increase > 0.2
}

func findLastChargeVoltage(rows []Reading) (v float32) {
	for _, r := range rows {
		if r.SemiCycle == constantCharge {
			v = r.VoltageV
		}
	}

	return v
}

func findFirstDischargeReading(rows []Reading) (v, c float32) {
	for _, r := range rows {
		if r.SemiCycle == constantDischarge {
			return r.VoltageV, r.CurrentA
		}
	}

	return 0, 0
}

func confidenceLevel(cyclesCount int) string {
	if cyclesCount < 5 {
		return "low"
	}

	return "high"
}

func getcurentShare(br [3]float32) [3]float32 {
	total := br[0] + br[1] + br[2]
	var share [3]float32

	for i, b := range br {
		share[i] = b / total
	}

	return share
}

func imbalanceFlag(currentShare [3]float32) bool {
	for _, share := range currentShare {
		if math.Abs(float64(share-(1.0/3.0))) > imbalanceThreshold {
			return true
		}
	}

	return false
}

func calcCycleImbalance(rows []Reading) bool {
	var totalP1, totalP2, totalP3 float32
	count := 0

	for _, r := range rows {
		if r.SemiCycle == constantDischarge {
			totalP1 += r.Branches[0].CurrentA
			totalP2 += r.Branches[1].CurrentA
			totalP3 += r.Branches[2].CurrentA

			count++
		}
	}

	if count == 0 {
		return false
	}

	avg := [3]float32{totalP1 / float32(count), totalP2 / float32(count), totalP3 / float32(count)}

	share := getcurentShare(avg)

	return imbalanceFlag(share)
}
