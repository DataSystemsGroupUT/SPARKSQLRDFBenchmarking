set terminal pdf font "Arial"
set output "plots.pdf"

set key outside bottom center horizontal width 3
set yrange [0:3]
set grid ytics
set xtics nomirror
set style fill pattern border
set boxwidth 0.75

set xtics ("avro" 2, "csv" 6, "orc" 10, "Parquet" 14)
set ylabel "Ratios of WPT are better than PT"
plot '1_val.dat' every 3::0 using 1:2 with boxes ls 1 lt -1 title 'NO' ,\
     '1_val.dat' every 3::1 using 1:2 with boxes ls 2 lt -1 title 'H' ,\
     '1_val.dat' every 3::2 using 1:2 with boxes ls 3 lt -1 title 'S',\
     '1_avg.dat' using 1:2 with lines ls 4 lw 5 lt -1 title 'AVG'

set yrange [0:1.8]
plot '2_val.dat' every 3::0 using 1:2 with boxes ls 1 lt -1 title 'NO' ,\
     '2_val.dat' every 3::1 using 1:2 with boxes ls 2 lt -1 title 'H' ,\
     '2_val.dat' every 3::2 using 1:2 with boxes ls 3 lt -1 title 'S',\
     '2_avg.dat' using 1:2 with lines ls 4 lw 5 lt -1 title 'AVG'

plot '3_val.dat' every 3::0 using 1:2 with boxes ls 1 lt -1 title 'NO' ,\
     '3_val.dat' every 3::1 using 1:2 with boxes ls 2 lt -1 title 'H' ,\
     '3_val.dat' every 3::2 using 1:2 with boxes ls 3 lt -1 title 'S',\
     '3_avg.dat' using 1:2 with lines ls 4 lw 5 lt -1 title 'AVG'

set xtics ("avro" 2.5, "csv" 7.5, "orc" 12.5, "Parquet" 17.5, "Hive" 22.5)
set yrange [0:1.4]
set ylabel "Ratios of ExtVP are better than VP"
plot '4_val.dat' every 3::0 using 1:2 with boxes ls 1 lt -1 title 'NO' ,\
     '4_val.dat' every 3::1 using 1:2 with boxes ls 2 lt -1 title 'H' ,\
     '4_val.dat' every 3::2 using 1:2 with boxes ls 3 lt -1 title 'P',\
     '4_val.dat' every 3::3 using 1:2 with boxes ls 6 lt -1 title 'S' ,\
     '4_avg.dat' using 1:2 with lines ls 4 lw 5 lt -1 title 'AVG'

set xtics ("avro" 2.5, "csv" 7.5, "orc" 12.5, "Parquet" 17.5)
set ylabel "Ratios of ExtVP are better than VP"
plot '5_val.dat' every 3::0 using 1:2 with boxes ls 1 lt -1 title 'NO' ,\
     '5_val.dat' every 3::1 using 1:2 with boxes ls 2 lt -1 title 'H' ,\
     '5_val.dat' every 3::2 using 1:2 with boxes ls 3 lt -1 title 'P',\
     '5_val.dat' every 3::3 using 1:2 with boxes ls 6 lt -1 title 'S' ,\
     '5_avg.dat' using 1:2 with lines ls 4 lw 5 lt -1 title 'AVG'
