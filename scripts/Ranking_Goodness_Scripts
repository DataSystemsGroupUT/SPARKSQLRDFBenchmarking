function minOperations(arr1, arr2,	i, j, n)
{
	
	// Base Case
	let f = 0;
	for(let i = 0; i < n; i++)
	{
		if (arr1[i] != arr2[i])
		  f = 1;
		  break;
	}
	if (f == 0)
		return 0;
	
	if (i >= n || j >= n)
		return 0;
	
	// If arr[i] < arr[j]
	if (arr1[i] < arr2[j])
	
		// Include the current element
		return 1 + minOperations(arr1, arr2,
								i + 1, j + 1, n);
	
	// Otherwise, excluding the current element
	return Math.max(minOperations(arr1, arr2,
								i, j + 1, n),
					minOperations(arr1, arr2,
								i + 1, j, n));
}

function minOperationsUtil2(arr, brr, n)
{
	
  return minOperations(arr, brr,	0, 0, n);
	
}

function refSort(target, ref) {

    targetData = target[0];
    refData = ref[0];

    // Create an array of indices [0, 1, 2, ...N].
    var indices = Object.keys(refData);

    // Sort array of indices according to the reference data.
    indices.sort(function(indexA, indexB) {
      if (refData[indexA] < refData[indexB]) {
        return -1;
      } else if (refData[indexA] > refData[indexB]) {
        return 1;
      }
      return 0;
    });

    // Map array of indices to corresponding values of the target array.
    return indices.map(function(index) {
      return targetData[index];
    });
}


function minOperationsUtil(ref, tar){

  arr = ref[0];
  n = arr.length;

  let brr = new Array(n);
	
	for(let i = 0; i < n; i++)
		brr[i] = arr[i];
	
	//brr.sort();

  return minOperationsUtil2(arr, refSort(brr,tar), n);
}

function hammingDistance(w, m) {
    word = w[0];
    matchingWord=m[0];
    let count = 0;

        if (word.length === matchingWord.length) {
          for (let i = 0; i <= word.length; i++) {
            if (word[i] !== matchingWord[i]) {
              count++;
            }
          }

          return count;
        }
        return "unequal word lengths";
}

function parr(input){
  return pairs([input]);f
}

function pairs(input){
  array=input[0]
  let results = [];
  for (let i = 0; i < array.length - 1; i++) {
  // This is where you'll capture that last value
    for (let j = i + 1; j < array.length; j++) {
      results.push([array[i],array[j]]);
    }
  }
  return results;
}

function myIndexOf(arr,val){
    return arr[0].indexOf(val);
  
}

function findEqRank(rank0, rank1){
  r = rank0[0];
  rb = rank1[0];
  res = [];
  for (let j = 0; j < r.length; j++) {
      p = r[j];
      if(j === rb.indexOf(p)) {
             res.push(p);
          }         
  }
  
  return res;
}

function invkendall(rank0, rank1, base){

  r = rank0[0];
  rb = rank1[0];
  parr = pairs(base);
  count = r.length*(r.length-1)/2;
  c = r.length*(r.length-1)/2;
  for (let j = 0; j < c; j++) {

      pair = parr[j];

      if(r.indexOf(pair[0]) === r.indexOf(pair[1])) {
          if(rb.indexOf(pair[0]) === rb.indexOf(pair[1])) {
             count = count ;
          }         
      }

      if(r.indexOf(pair[0]) > r.indexOf(pair[1])) {
          if(rb.indexOf(pair[0]) < rb.indexOf(pair[1])) {
            count = count - 1;
          }
          else
            count = count ;
      }

      if(r.indexOf(pair[0]) < r.indexOf(pair[1])) {
          if(rb.indexOf(pair[0]) > rb.indexOf(pair[1])) {
            count = count - 1;
          } 
          else
            count = count ;
      }
  }

  return count;
}

//weighted kendall based on the position
function wkendall(rank0, rank1, base){

  r = rank0[0];
  rb = rank1[0];
  parr = pairs(base);
  count = 0;
  c = r.length*(r.length-1)/2;
  for (let j = 0; j < c; j++) {

      pair = parr[j];

      k1 = Math.min(r.indexOf(pair[0]),r.indexOf(pair[1]), c/2);
      k2 = Math.min(rb.indexOf(pair[0]),rb.indexOf(pair[1]), c/2);

      if(r.indexOf(pair[0]) === r.indexOf(pair[1])) {
          if(rb.indexOf(pair[0]) === rb.indexOf(pair[1])) {
             count = count ;
          }         
      }

      if(r.indexOf(pair[0]) > r.indexOf(pair[1])) {
          if(rb.indexOf(pair[0]) < rb.indexOf(pair[1])) {
            count = count + ( k1-k2)/k2*k1;
          }
          else
            count = count ;
      }

      if(r.indexOf(pair[0]) < r.indexOf(pair[1])) {
          if(rb.indexOf(pair[0]) > rb.indexOf(pair[1])) {
            count = count + ( k1-k2)/k2*k1;
          } 
          else
            count = count ;
      }
  }

  return count;
}



//ranking ordinale
function myrank(rank0, rank1){

  r = rank0[0];
  rb = rank1[0];
  pairs = parr(union(r,rb));
  count = 0;
  c = pairs.length;

  var indexOf = function indexOf(r,e){
       return r.indexOf(e)!=-1 ?
          r.indexOf(e) : c ;
  }

  for (let j = 0; j < c; j++) {

        pair = pairs[j]; 
        if(indexOf(r,pair[0]) == indexOf(r,pair[1])) {
          if(indexOf(rb,pair[0]) == indexOf(rb,pair[1])) {
             count = count ;
          } else
            count = count +1
        }
        else if(indexOf(r,pair[0]) > indexOf(r,pair[1])) {
            if(indexOf(rb,pair[0]) <= indexOf(rb,pair[1])) {
              count = count + 1;
            }
            else
              count = count ;
        }
        else if(indexOf(r,pair[0]) < indexOf(r,pair[1])) {
          if(indexOf(rb,pair[0]) >= indexOf(rb,pair[1])) {
            count = count + 1;
          } 
          else
            count = count ;
        }

  }

  return count;

}



function lcs(set1m, set2m) {
  set1 = set1m[0];
  set2 = set2m[0];
  // Init LCS matrix.
  const lcsMatrix = Array(set2.length + 1).fill(null).map(() => Array(set1.length + 1).fill(null));

  // Fill first row with zeros.
  for (let columnIndex = 0; columnIndex <= set1.length; columnIndex += 1) {
    lcsMatrix[0][columnIndex] = 0;
  }

  // Fill first column with zeros.
  for (let rowIndex = 0; rowIndex <= set2.length; rowIndex += 1) {
    lcsMatrix[rowIndex][0] = 0;
  }

  // Fill rest of the column that correspond to each of two strings.
  for (let rowIndex = 1; rowIndex <= set2.length; rowIndex += 1) {
    for (let columnIndex = 1; columnIndex <= set1.length; columnIndex += 1) {
      if (set1[columnIndex - 1] === set2[rowIndex - 1]) {
        lcsMatrix[rowIndex][columnIndex] = lcsMatrix[rowIndex - 1][columnIndex - 1] + 1;
      } else {
        lcsMatrix[rowIndex][columnIndex] = Math.max(
          lcsMatrix[rowIndex - 1][columnIndex],
          lcsMatrix[rowIndex][columnIndex - 1],
        );
      }
    }
  }

  // Calculate LCS based on LCS matrix.
  if (!lcsMatrix[set2.length][set1.length]) {
    // If the length of largest common string is zero then return empty string.
    return [''];
  }

  const longestSequence = [];
  let columnIndex = set1.length;
  let rowIndex = set2.length;

  while (columnIndex > 0 || rowIndex > 0) {
    if (set1[columnIndex - 1] === set2[rowIndex - 1]) {
      // Move by diagonal left-top.
      longestSequence.unshift(set1[columnIndex - 1]);
      columnIndex -= 1;
      rowIndex -= 1;
    } else if (lcsMatrix[rowIndex][columnIndex] === lcsMatrix[rowIndex][columnIndex - 1]) {
      // Move left.
      columnIndex -= 1;
    } else {
      // Move up.
      rowIndex -= 1;
    }
  }

  return longestSequence.length;
};


//jacard

function intersection(a, b) {
  var x = [];
  var check = function (e, cb) {
    if (~b.indexOf(e)) x.push(e);
    if (cb && typeof cb == 'function') cb(null);
  };

  a.forEach(check);
  return x;
 
}

function inersect(inputa,inputb){
  return intersection(inputa[0],inputb[0]);
}

function unn(inputa,inputb){
  return union(inputa[0],inputb[0]);
}

function union_size(inputa,inputb){
  return union(inputa[0],inputb[0]).length;
}

function pair_size(inputa,inputb){
  u = union(inputa[0],inputb[0]);
  return u.length * (u.length -1)/2;
}


/*
 * Return distinct elements from both input sets
 */
function union(a, b) {
  var x = [];
  var check = function (e, cb) {
    if (!~x.indexOf(e)) x.push(e);
    if (cb && typeof cb == 'function') cb(null);
  }

  a.forEach(check);
  b.forEach(check);
  return x;
  
}

/*
 * Similarity
 */
function jacard (arr, brr, threshold) {

    if(threshold){
      a = arr[0].slice(0, threshold);
      b = brr[0].slice(0, threshold);
    }
    else{
      a = arr[0];
      b = brr[0];
    }

    return intersection(a, b).length / union(a, b).length;
}



    

