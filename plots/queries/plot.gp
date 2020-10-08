set terminal pdf font "Arial"
set output "plot_wpt.pdf"

# set grid ytics
set key inside top center horizontal width 3
set xtics nomirror
set style fill pattern border
set xrange [0.5:9.5]
set yrange [0:200]

set xtics ("Q1" 1, "Q2" 2, "Q3" 3, "Q4" 4, "Q5" 5, "Q6" 6, "Q8" 7, "Q10" 8, "Q11" 9)
set ylabel "Time (seconds)"
plot 'wpt_csv.dat' using 1:2 w lp ls 2 lc -1 title 'WPT_H' ,\
     'wpt_csv.dat' using 1:3 w lp ls 4 lc -1 title 'PT_H' ,\
     'wpt_csv.dat' using 1:4 w lp ls 5 lc -1 title 'WPT_S' ,\
     'wpt_csv.dat' using 1:5 w lp ls 6 lc -1 title 'PT_S'
plot 'wpt_csv.dat' using 1:2 w lp ls 2 lc -1 title 'WPT_H' ,\
     'wpt_csv.dat' using 1:3 w lp ls 4 lc -1 title 'PT_H'
plot 'wpt_csv.dat' using 1:4 w lp ls 5 lc -1 title 'WPT_S' ,\
     'wpt_csv.dat' using 1:5 w lp ls 6 lc -1 title 'PT_S'


plot 'wpt_avro.dat' using 1:2 w lp ls 2 lc -1 title 'WPT' ,\
     'wpt_avro.dat' using 1:3 w lp ls 4 lc -1 title 'PT' ,\
     'wpt_avro.dat' using 1:4 w lp ls 5 lc -1 title 'WPT' ,\
     'wpt_avro.dat' using 1:5 w lp ls 6 lc -1 title 'PT'
plot 'wpt_avro.dat' using 1:2 w lp ls 2 lc -1 title 'WPT' ,\
     'wpt_avro.dat' using 1:3 w lp ls 4 lc -1 title 'PT'
plot 'wpt_avro.dat' using 1:4 w lp ls 5 lc -1 title 'WPT' ,\
     'wpt_avro.dat' using 1:5 w lp ls 6 lc -1 title 'PT'


plot 'wpt_orc.dat' using 1:2 w lp ls 2 lc -1 title 'WPT' ,\
     'wpt_orc.dat' using 1:3 w lp ls 4 lc -1 title 'PT' ,\
     'wpt_orc.dat' using 1:4 w lp ls 5 lc -1 title 'WPT' ,\
     'wpt_orc.dat' using 1:5 w lp ls 6 lc -1 title 'PT'
plot 'wpt_orc.dat' using 1:2 w lp ls 2 lc -1 title 'WPT' ,\
     'wpt_orc.dat' using 1:3 w lp ls 4 lc -1 title 'PT'
plot 'wpt_orc.dat' using 1:4 w lp ls 5 lc -1 title 'WPT' ,\
     'wpt_orc.dat' using 1:5 w lp ls 6 lc -1 title 'PT'


plot 'wpt_parquet.dat' using 1:2 w lp ls 2 lc -1 title 'WPT' ,\
     'wpt_parquet.dat' using 1:3 w lp ls 4 lc -1 title 'PT' ,\
     'wpt_parquet.dat' using 1:4 w lp ls 5 lc -1 title 'WPT' ,\
     'wpt_parquet.dat' using 1:5 w lp ls 6 lc -1 title 'PT'
plot 'wpt_parquet.dat' using 1:2 w lp ls 2 lc -1 title 'WPT' ,\
     'wpt_parquet.dat' using 1:3 w lp ls 4 lc -1 title 'PT'
plot 'wpt_parquet.dat' using 1:4 w lp ls 5 lc -1 title 'WPT' ,\
     'wpt_parquet.dat' using 1:5 w lp ls 6 lc -1 title 'PT'

# ---------------------------------------------
set yrange [0:500]
set output "plot_extvp.pdf"

set xtics ("Q1" 1, "Q2" 2, "Q3" 3, "Q4" 4, "Q5" 5, "Q6" 6, "Q8" 7, "Q9" 8, "Q10" 9, "Q11" 10)
set xlabel "HO"
plot 'extvp_parquet.dat' using 1:2 w lp ls 2 lc -1 title 'ExtVP' ,\
     'extvp_parquet.dat' using 1:3 w lp ls 4 lc -1 title 'VP'
set xlabel "Subj"
plot 'extvp_parquet.dat' using 1:4 w lp ls 2 lc -1 title 'ExtVP' ,\
     'extvp_parquet.dat' using 1:5 w lp ls 4 lc -1 title 'VP'
set xlabel "Pred"
plot 'extvp_parquet.dat' using 1:6 w lp ls 2 lc -1 title 'ExtVP' ,\
     'extvp_parquet.dat' using 1:7 w lp ls 4 lc -1 title 'VP'

set xlabel "HO"
plot 'extvp_orc.dat' using 1:2 w lp ls 2 lc -1 title 'ExtVP' ,\
     'extvp_orc.dat' using 1:3 w lp ls 4 lc -1 title 'VP'
set xlabel "Subj"
plot 'extvp_orc.dat' using 1:4 w lp ls 2 lc -1 title 'ExtVP' ,\
     'extvp_orc.dat' using 1:5 w lp ls 4 lc -1 title 'VP'
set xlabel "Pred"
plot 'extvp_orc.dat' using 1:6 w lp ls 2 lc -1 title 'ExtVP' ,\
     'extvp_orc.dat' using 1:7 w lp ls 4 lc -1 title 'VP'

set xlabel "HO"
plot 'extvp_avro.dat' using 1:2 w lp ls 2 lc -1 title 'ExtVP' ,\
     'extvp_avro.dat' using 1:3 w lp ls 4 lc -1 title 'VP'
set xlabel "Subj"
plot 'extvp_avro.dat' using 1:4 w lp ls 2 lc -1 title 'ExtVP' ,\
     'extvp_avro.dat' using 1:5 w lp ls 4 lc -1 title 'VP'
set xlabel "Pred"
plot 'extvp_avro.dat' using 1:6 w lp ls 2 lc -1 title 'ExtVP' ,\
     'extvp_avro.dat' using 1:7 w lp ls 4 lc -1 title 'VP'

set xlabel "HO"
plot 'extvp_csv.dat' using 1:2 w lp ls 2 lc -1 title 'ExtVP' ,\
     'extvp_csv.dat' using 1:3 w lp ls 4 lc -1 title 'VP'
set xlabel "Subj"
plot 'extvp_csv.dat' using 1:4 w lp ls 2 lc -1 title 'ExtVP' ,\
     'extvp_csv.dat' using 1:5 w lp ls 4 lc -1 title 'VP'
set xlabel "Pred"
plot 'extvp_csv.dat' using 1:6 w lp ls 2 lc -1 title 'ExtVP' ,\
     'extvp_csv.dat' using 1:7 w lp ls 4 lc -1 title 'VP'
