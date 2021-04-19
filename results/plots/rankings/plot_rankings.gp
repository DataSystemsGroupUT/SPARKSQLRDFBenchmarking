set terminal pdf font "Arial,14"

set key outside bottom center horizontal width 5 spacing 1
set ylabel "Rank Score"

set auto x
set grid ytics
set yrange [0:1]
set style data histogram
set style histogram cluster gap 2
set style fill solid border
set boxwidth 0.9
set xtic scale 0

# Storage Backends

set output "storage_100_ST.pdf"
plot 'storage_100_ST.dat' using 2:xtic(1) ti col ,\
     '' u 3 ti col ,\
     '' u 4 ti col ,\
     '' u 5 ti col ,\
     '' u 6 ti col ,\

set output "storage_100_VT.pdf"
plot 'storage_100_VT.dat' using 2:xtic(1) ti col ,\
     '' u 3 ti col ,\
     '' u 4 ti col ,\
     '' u 5 ti col ,\
     '' u 6 ti col ,\

set output "storage_100_PT.pdf"
plot 'storage_100_PT.dat' using 2:xtic(1) ti col ,\
     '' u 3 ti col ,\
     '' u 4 ti col ,\
     '' u 5 ti col ,\
     '' u 6 ti col ,\

set output "storage_500_ST.pdf"
plot 'storage_500_ST.dat' using 2:xtic(1) ti col ,\
     '' u 3 ti col ,\
     '' u 4 ti col ,\
     '' u 5 ti col ,\
     '' u 6 ti col ,\

set output "storage_500_VT.pdf"
plot 'storage_500_VT.dat' using 2:xtic(1) ti col ,\
     '' u 3 ti col ,\
     '' u 4 ti col ,\
     '' u 5 ti col ,\
     '' u 6 ti col ,\

set output "storage_500_PT.pdf"
plot 'storage_500_PT.dat' using 2:xtic(1) ti col ,\
     '' u 3 ti col ,\
     '' u 4 ti col ,\
     '' u 5 ti col ,\
     '' u 6 ti col ,\

# Partitioning Techniques

set output "partitioning_100_ST.pdf"
plot 'partitioning_100_ST.dat' using 2:xtic(1) ti col ,\
     '' u 3 ti col ,\
     '' u 4 ti col ,\

set output "partitioning_100_VT.pdf"
plot 'partitioning_100_VT.dat' using 2:xtic(1) ti col ,\
     '' u 3 ti col ,\
     '' u 4 ti col ,\

set output "partitioning_100_PT.pdf"
plot 'partitioning_100_PT.dat' using 2:xtic(1) ti col ,\
     '' u 3 ti col ,\
     '' u 4 ti col ,\

set output "partitioning_500_ST.pdf"
plot 'partitioning_500_ST.dat' using 2:xtic(1) ti col ,\
     '' u 3 ti col ,\
     '' u 4 ti col ,\

set output "partitioning_500_VT.pdf"
plot 'partitioning_500_VT.dat' using 2:xtic(1) ti col ,\
     '' u 3 ti col ,\
     '' u 4 ti col ,\

set output "partitioning_500_PT.pdf"
plot 'partitioning_500_PT.dat' using 2:xtic(1) ti col ,\
     '' u 3 ti col ,\
     '' u 4 ti col ,\

# Relational schemas

set output "schemas_100_HP.pdf"
plot 'schemas_100_HP.dat' using 2:xtic(1) ti col ,\
     '' u 3 ti col ,\
     '' u 4 ti col ,\

set output "schemas_100_SBP.pdf"
plot 'schemas_100_SBP.dat' using 2:xtic(1) ti col ,\
     '' u 3 ti col ,\
     '' u 4 ti col ,\


set output "schemas_100_PBP.pdf"
plot 'schemas_100_PBP.dat' using 2:xtic(1) ti col ,\
     '' u 3 ti col ,\
     '' u 4 ti col ,\

set output "schemas_500_HP.pdf"
plot 'schemas_500_HP.dat' using 2:xtic(1) ti col ,\
     '' u 3 ti col ,\
     '' u 4 ti col ,\

set output "schemas_500_SBP.pdf"
plot 'schemas_500_SBP.dat' using 2:xtic(1) ti col ,\
     '' u 3 ti col ,\
     '' u 4 ti col ,\


set output "schemas_500_PBP.pdf"
plot 'schemas_500_PBP.dat' using 2:xtic(1) ti col ,\
     '' u 3 ti col ,\
     '' u 4 ti col ,\
