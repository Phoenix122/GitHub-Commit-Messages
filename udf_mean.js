// UDF definition
function findMean(row, emit) {
  emit({mean: Mean(JSON.parse(row.pLoad))
        }
      );
}

function Mean(obj) {
  var sum = 0;
  var cnt = 0;
  
    for(var field in obj) {
      sum += obj[field].message.length;
      cnt++;
    }
  return (cnt == 0)?0:sum/cnt;
}

// UDF registration
bigquery.defineFunction(
  'findMean',  // Name used to call the function from SQL

  ['pLoad'],  // Input column names

  // JSON representation of the output schema
  [{name: 'mean', type: 'integer'}
  ],

  findMean  // The function reference
);