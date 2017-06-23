// UDF definition
function findMedian(row, emit) {
  emit({median: Median(JSON.parse(row.pLoad))
        }
      );
}

//Median function
function Median(obj) {
  var lengths = [0]; 
  for(var field in obj) {
    lengths.push(obj[field].message.length);
  }
  lengths.sort( function(a,b) {return a - b;} );

    var mid = Math.floor(lengths.length/2);

    if(lengths.length % 2)
        return lengths[mid];
    else
        return (lengths[mid-1] + lengths[mid]) / 2.0;
}
  
// UDF registration
bigquery.defineFunction(
  'findMedian',  // Name used to call the function from SQL

  ['pLoad'],  // Input column names

  // JSON representation of the output schema
  [{name: 'median', type: 'integer'}
  ],

  findMedian  // The function reference
);