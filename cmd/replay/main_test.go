package main

import (
	"math"
	"testing"
)

func TestPackCycleReplay(t *testing.T) {
	const epsilon = 1e-5 // tolerence

	t.Run("internal resistance calculation", func(t *testing.T) {
		rows := []Reading{
			{
				SemiCycle: "Capacity check Charge",
				VoltageV: 14.4,
			},
			{
				SemiCycle: "Capacity check Discharge",
				VoltageV: 13.9,
				CurrentA: 10.0,
			},

		}

		resitance, ok := calcInternalResistance(rows)

		if !ok {
			t.Errorf("expect ok == true, got %v ", ok)
		}

		var expectedR float32 = (14.4 - 13.9) / 10.0

		if math.Abs(float64(resitance-expectedR)) > epsilon {
			t.Errorf("expected resistance: %v, got %v", expectedR, resitance)
		}
	})


	t.Run("resistance flag", func(t *testing.T) {
		cases := []struct{
			history []float32
			latest float32
			want bool
		}{
			{

				history: []float32{0.04, 0.045, 0.05, 0.042},
				latest: float32(0.06),
				want: true,
			},
			{
				history: []float32{0.05, 0.05, 0.05, 0.05},
				latest:  float32(0.0575),
				want:    false,
			},
			{
				history: []float32{0.05, 0.05, 0.05, 0.05},
				latest:  float32(0.0625),
				want:    true,
			},
		}

		for _, v := range cases {
			rflag := resistanceFlag(v.latest, v.history)

			if rflag != v.want {
				t.Errorf("expect rlfag == %v, got %v", v.want, rflag)
			}
		}


	})

	t.Run("test cycle imbalance calcualtion", func(t *testing.T) {
		cases := []struct{
			reading []Reading
			want bool
		}{
			{
				reading: []Reading{
					{
						SemiCycle: "Capacity check Discharge",
						Branches: [3]BranchReading{
							{CurrentA: 3.5},
							{CurrentA: 3.4},
							{CurrentA: 2.1},
						},
					},
				},
				want: true,
			},
			{
				reading: []Reading{
					{SemiCycle: "WLTP", Branches: [3]BranchReading{{CurrentA: 5}, {CurrentA: 5}, {CurrentA: 5}}},
				},
				want: false,
			},
		}

		for _, v := range cases {
			iflag := calcCycleImbalance(v.reading)

			if iflag != v.want {
				t.Errorf("expected: %v, got: %v", v.want, iflag)
			}
		}
	})


}
