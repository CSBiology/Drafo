(**
## seriesToFrame

The function `seriesToFrame` takes a Series  of the type <Key,'a> and tunes it into a Frame of the type <int,string>.
The function applies the value part of the `Key` as values and the value of the series tuple into its own Coumn called `Values`
To get the Key type back in the Frame you can vor example index it with the `indexWithColumnValues` function

*)
let newKeyTest str a = Key().addCol(str,a)
let seriesForFrame:Series<Key,_> = 
    [
        (newKeyTest "One" 4.).addCol ("Two",2.), 1.
        (newKeyTest "One" 2.).addCol ("Two",5.), 2.
    ]
    |>series
let seriesToFrameTest = seriesToFrame seriesForFrame 
seriesToFrameTest.Print()(* output: 
One Two Value 
0 -> 4   2   1     
1 -> 2   5   2*)
(**
## indexWithColumnValues

The function `indexWithColumnValues` takes the Column Keys defined in a Series that is provided and Values in these columns and applies them as Keys into the Rows. The originell column keys are maintained.

*)
let seriesToFrameTestIndexed = seriesToFrame seriesForFrame |> indexWithColumnValues ["One"; "Two"]
seriesToFrameTestIndexed.Print()(* output: 
One Two Value 
One: 4
Two: 2
 -> 4   2   1     
One: 2
Two: 5
 -> 2   5   2*)
(**
## getColumn

the function of the `getColumn` function is the equal to the the same operation in deedle.

*)
let getColumn2: Series<Key,string> = getColumn "Two" seriesToFrameTestIndexed
getColumn2.Print()(* output: 
One: 4
Two: 2
 -> 2 
One: 2
Two: 5
 -> 5*)
(**
## rowKeyToColumns

The function `rowKeyoColumns` shifts the row keys of type Key into the columns and become strings. The value part of the Key type get put into the new colums

*)
let exmpColMajorFrameTwo =
    frame [
        ( "c1,T1,r1" )=> series [(newKeyTest"row1"  1.).addCol ("row2", 3.),3.]
        ( "c2,T1,r1" )=> series [(newKeyTest"row1"  10.).addCol("row2" ,100.),100.]
    ]
exmpColMajorFrameTwo.Print()

let testRowToColumnKey = rowKeyToColumns exmpColMajorFrameTwo

testRowToColumnKey.Print() (* output: 
c1,T1,r1  c2,T1,r1  
row1: 1
row2: 3
    -> 3         <missing> 
row1: 10
row2: 100
 -> <missing> 100       

     row1 row2 c1,T1,r1  c2,T1,r1  
0 -> 1    3    3         <missing> 
1 -> 10   100  <missing> 100*)
(**
## createFilter

The function `createFilter` needs a function that takes a input and gives a bool and a series of <Key,_>(most often a single column). It then gives a Series of <Key,bool>
that can be used in later functions as filter.

*)
let boolFunction a =    
    if a > 1. then true
    else false
let letsCreateAFilter = createFilter boolFunction seriesForFrame
letsCreateAFilter.Print()(* output: 
One: 4
Two: 2
 -> False 
One: 2
Two: 5
 -> True*)
(**
## transform

The `transform`function needs a function that turns 'a into 'b and a series that has the <Key,_> type (most often a single column) to transform your data

*)
let transformFuction a= 
    if a = 1. then 0.
    else a
let letsTransformSomeSeries = transform transformFuction seriesForFrame
letsTransformSomeSeries.Print()(* output: 
One: 4
Two: 2
 -> 0 
One: 2
Two: 5
 -> 2*)
(**
## zip

the `zip` function needs a function that takes two parameters and two series of type <Keytype,'a> (most often a single column) which then zippes depending on the function used

*)
let getColumnOne = exmpColMajorFrameTwo|> getColumn<float> "c1,T1,r1"
let getColumnTwo=  exmpColMajorFrameTwo|> getColumn<float> "c2,T1,r1"

let zipped = zip (fun x y -> x / y) getColumnOne getColumnTwo
zipped.Print()(* output: 
row1: 1
row2: 3
    -> <missing> 
row1: 10
row2: 100
 -> <missing>*)
(**
## dropKeyColumns and dropAllKeyColumnsBut

`dropKeyColumns` and `dropAllKeyColumnsBut` takes a sequence of column keys and a Key.
the seqeunces should in the first case contain all columns that need to be dropped and in the second option all colums that will be kept
// the Key can contain any number of keys from which keys can be dropped

*)
let seriesForPropertyDrop  = seq ["c1,T1,r1";"c2,T1,r1";"c2,T1,r3"]
let keyForPropertyDrop = ( newKeyTest "c1,T1,r1"  0).addCol ("LLLL",9)

let dropsProperty =  dropKeyColumns seriesForPropertyDrop keyForPropertyDrop
printfn("%O")dropsProperty(* output: 
LLLL: 9*)
let dropsAllPropBut =dropAllKeyColumnsBut seriesForPropertyDrop keyForPropertyDrop
printfn("%O") dropsAllPropBut(* output: 
c1,T1,r1: 0*)
(**
## group functions

The `groupTransform` functions takes a function of `op :'a [] -> 'a -> 'b`, in this case the dropkey function a `Seq<string> and a Series<'KeyType, 'a>`.
Depending on the function used the series gets transformed. While the `groupFilter` is a special version of it that uses groupTransform and needs
`op :'a [] -> 'a -> bool`.

*)
let seriesForFrameFloat:Series<Key,_> = 
    [
        (newKeyTest "Two" 4).addCol ("One",2), 1.
        (newKeyTest "Two" 2).addCol ("One",5), 2.
        (newKeyTest "Two" 2).addCol ("One",7), 3. 
    ]
    |>series

let seriesForPropertyDropMod  = seq ["Two"]

let operation =                 
    fun (x:seq<float>) -> 
        let m = Seq.mean x
        fun x -> x - m
    
let tryGroupsTransform = groupTransform operation dropAllKeyColumnsBut seriesForPropertyDropMod seriesForFrameFloat

tryGroupsTransform.Print()(* output: 
Two: 4
One: 2
 -> 0    
Two: 2
One: 5
 -> -0.5 
Two: 2
One: 7
 -> 0.5*)
let opFilter= 
    fun values -> 
        let mean = Seq.mean values
        (fun values  -> values <= mean)
    
let tryGroupsFilter = createGroupFilter opFilter dropAllKeyColumnsBut seriesForPropertyDropMod seriesForFrameFloat

tryGroupsFilter.Print()(* output: 
Two: 4
One: 2
 -> True  
Two: 2
One: 5
 -> True  
Two: 2
One: 7
 -> False*)
(**
## aggregate

The `aggregate` function uses a created filter to filter the given series of <Key,'a> and everyhing that is not true is dropped. In this case only the first row is kept.
Then either `dropKeyColumns` or `dropAllKeyColumnsBut` or a user defined function is applied to the filtered series that was turned into a frame. In the end
the op function is applied

*)
let letsTransformAFilter = seq [(createFilter boolFunction seriesForFrameFloat)]
    
let op =fun (x:seq<float>) -> Seq.mean x

let aggregations = aggregate op dropAllKeyColumnsBut seriesForPropertyDropMod letsTransformAFilter seriesForFrameFloat

aggregations.Print()(* output: 
Two: 2
 -> 2.5*)
(**
## assemble

`assemble` takes a sequence of Series and creates a frame

*)
let forKeySeriesOne:Series<_,int> = seq [(newKeyTest"Two" 1).addCol("T",2), 6;(newKeyTest"Two" 3).addCol("T",5), 9]|>series

let forKeySeriesTwo:Series<_,int> = seq [(newKeyTest"Two" 1).addCol("T",2), 6;(newKeyTest"Two" 3).addCol("T",5), 9]|>series

let assembly = 
    assemble 
        [
            "One",forKeySeriesOne:>ISeries<Key>
            "Two",forKeySeriesTwo:>ISeries<Key>
        ]

assembly.Print()(* output: 
One Two 
Two: 1
T: 2
 -> 6   6   
Two: 3
T: 5
 -> 9   9*)
(**
## pivot

`pivot` is a function that takes a string and a Frame. The string decides which row `Key` is applied to the column key, the value part is applied.
The values in rows that were deleted are moved into the still existing rows

*)
let testString ="One"

let testPivot = pivot testString  seriesToFrameTestIndexed

testPivot.Print()(* output: 
4.One     4.Two     4.Value   2.One     2.Two     2.Value   
Two: 2
 -> 4         2         1         <missing> <missing> <missing> 
Two: 5
 -> <missing> <missing> <missing> 2         5         2*)
(**
## module NumericAggregation

`NumericAggregation` does either the mean, meadian or float based on your input. For that it needs a Frame
of <Key,_> a fliter, a `<seq <Series <Key,bool>>>` or a `<seq <seq <Series <Key,bool>>>>`, that say which values should be aggregated and for a singel column a string
The module has two functions either `numAgAllCol`that aggregates over all columns or `numAggregat`that agggregates one column
The result is a Frame<Key,string>

## module NumericFilter

`NumericFilter` is a module that can say if values in a given column are bigger or smaller than a given value.
The resultant series of the type <Key,bool>.
The input Frame needs to be of the type Frame<Key,_>.
The module has one function for all columns in a frame :`numericFilterServeralCol` and one for a single `columnnumericFilter`which also needs a string to determine the column

## module NumericTransform

The `NumericTransform` has five operations that can be done on all values in either a column(sereies) or a frame:
| Log2
| Substract
| Add
| DivideBy
| MultiplyBy

These are can be either used on a full frame,`numericTransformAllCols` , or on a single column, `numericTransformOneCol`.
Both need a Frame of the type <Key,_> and a seq<string>, to determine which columns are dropped but  `numericTransform`
needs a string to determine the column.

## moduleGroupWiseNumericTransform

The `GroupWiseNumericTransform` has four operations:
| DivideByMedian
| DivideByMean
| SubstractMedian
| SubstractMean
These are can be either used on a full frame,`groupWiseNumericTransformAllCols` , or on a single column, `groupWiseNumericTransform`.
Both need a Frame of the type <Key,_> and a seq<string>, to determine which columns are dropped but  `groupWiseNumericTransform`
needs a string to determine the column.

## module GroupFilter

`Groupfilter` filters the given Frame of  <Key,_>. If a value of a Series (column), is either a bigger or lower than the given Tukey or stdev of the series than the bool is <false> otherwise it is <true>
The end result is a Series of  Key true/false pairs or a seq of series <Key,bool> for the variant that iterats over the entire frame.
`groupFilter` only uses one column of a Frame and `groupFilterAllCol`iterats over the entire frame

## module StringAggregation

StrinAggregation concats string in the provied Series of <Key,string>.
This happens based on the filter provided.
`stAggregate` only uses one column of a Frame and `stAggregateFullFrame`iterats over the entire frame

*)

