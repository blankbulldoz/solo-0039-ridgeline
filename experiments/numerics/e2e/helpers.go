package main

import (
	"fmt"
	"runtime"
	"strconv"

	"ridgeline/internal/model"
)

var goVer = runtime.Version()

func strconvF(v float64) string { return strconv.FormatFloat(v, 'g', -1, 64) }
func itoa64(v int64) string     { return strconv.FormatInt(v, 10) }

func mustNS(_ model.Namespace, err error) {
	if err != nil {
		panic(err)
	}
}

func printFindings(r Report, path string) {
	fmt.Printf("Ridgeline end-to-end reducer experiments (%s)\n", r.GoVersion)
	fmt.Printf("machine-readable report: %s\n\n", path)
	for _, x := range r.Findings {
		fmt.Printf("[%s] %s\n  %s\n", x.Class, x.ID, x.Description)
		if x.Detail != "" {
			fmt.Printf("  detail: %s\n", x.Detail)
		}
		if x.RelErr != nil || x.AbsErr != 0 {
			fmt.Printf("  cell=%s reference=%s abs_err=%s rel_err=%v ulp=%v\n",
				strconvF(x.CellValue), strconvF(x.Reference), strconvF(x.AbsErr),
				x.RelErr, x.ULP)
		}
		fmt.Println()
	}
}
