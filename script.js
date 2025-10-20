const fs = require('fs');
const csv = require('csv-parser');

const rows = [];
const duplicates = [];



const DUPLICATE_THRESHOLD = 2;

fs.createReadStream('big_data.csv')
  .pipe(csv())
  .on('data', (row) => {
    rows.push(row);
  })
  .on('end', () => {
    const seenIndexes = new Set();

    for (let i = 0; i < rows.length; i++) {
      for (let j = i + 1; j < rows.length; j++) {
        const commonFields = Object.keys(rows[i]).filter(
          key => rows[i][key] === rows[j][key]
        );

        if (commonFields.length >= DUPLICATE_THRESHOLD) {
         
          if (!seenIndexes.has(i)) {
            duplicates.push({ index: i + 1, ...rows[i] });
            seenIndexes.add(i);
          }
          if (!seenIndexes.has(j)) {
            duplicates.push({ index: j + 1, ...rows[j] });
            seenIndexes.add(j);
          }
        }
      }
    }

    if (duplicates.length > 0) {
      console.log(`Βρέθηκαν ${duplicates.length} διπλότυπες γραμμές (με τουλάχιστον ${DUPLICATE_THRESHOLD} κοινά πεδία):`);
      console.table(duplicates);
    } else {
      console.log('Δεν βρέθηκαν διπλότυπα.');
    }
  });
