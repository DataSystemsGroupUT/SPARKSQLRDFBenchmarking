function transform(str) {
    const matrix = str.split("\n").map(row => row.split("\t").map(n => Number(n)));
    const ranks = matrix.map(row => {
        const sortedRow = Array.from(row).sort((a,b) => a-b);
        const ranks = row.map(v => sortedRow.indexOf(v));
        return ranks;
    });
    const res = new Array(ranks[0].length).fill(0).map(() => new Array(ranks[0].length).fill(0));
    ranks.forEach(row => {
            row.forEach((v, i) => {
                res[i][v]++;
            })
    });
    console.table(res);
}