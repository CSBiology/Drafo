module Tests

open Expecto


open Deedle
open Drafo.Core
open GroupWiseNumericTransform.GroupWiseNumericTransform
open NumericAggregation.NumericAggregation
open StringAggregation.StringAggregation
open GroupFilter.GroupFilter
open NumericFilter.NumericFilter
open FSharp.Stats
open FSharpAux


let testFrame = 
    frame [
        ("Column1")=> series ["row1" => 1;"row2" =>3]
        ("Column2")=> series ["row1" => 10;"row2" =>100]    
    ]
    |>Frame.addCol "keys"(series ["row1" => "key1" ;"row2" =>"key2"])
let testFrameTwo = 
    frame [
        ("Column1")=> series ["row1" => 1;"row2" =>3]
        ("Column2")=> series ["row1" => 10;"row2" =>100]    
    ]
    |>Frame.addCol "keys"(series ["row1" => "key1" ;"row2" =>"key1"])
    |>Frame.addCol "KeyPartTwo"(series ["row1" => "key2" ;"row2" =>"key3"])

let transformedFrame = indexWithColumnValues ["keys"] testFrame

let transformedTestFrameTwo = indexWithColumnValues ["keys";"KeyPartTwo"] testFrameTwo


let TestFrameWithOutColumnsForIndexing = 
    transformedTestFrameTwo
    |> Frame.dropCol "keys"
    |> Frame.dropCol "KeyPartTwo"




let creatF threshold x =
    let opFilter = fun values -> 
        let mean = Seq.mean values
        let stdev = Seq.stDev values
        (fun v -> v <= (mean+stdev*threshold) && v >= (mean-stdev*threshold)
        ) 
    let column= getColumn<float> "Column1" x
    let actualWOTh = createGroupFilter opFilter dropAllKeyColumnsBut ["Column1"] column
    actualWOTh

let colKeysForSeq seqToIt x= 
    seqToIt
    |>Seq.item x

let filterForTests =  numericFilter (IsBiggerThan 2.) transformedTestFrameTwo "Column1" 
let filterMulty = numericFilterServeralCol (IsBiggerThan 2.) TestFrameWithOutColumnsForIndexing 
let filterMultyPartTwo =numericFilterServeralCol (IsBiggerThan 2.5) TestFrameWithOutColumnsForIndexing
let seqOfFilterMulty =  
    filterMulty
    |>Seq.mapi (fun i x -> 
        let colKeysForSeq seqIt b= 
            seqIt
            |>Seq.item b
        let cIII = seq [x;colKeysForSeq filterMultyPartTwo i]
        cIII)

let TestFrameWithOutColumnsForIndexingForStrinAg = 
    transformedTestFrameTwo
    |> Frame.dropCol "Column2"
    |> Frame.dropCol "Column1"


[<Tests>]
let CoreTests =
    testList "CoreTests.TestOne" [

        testCase "getColumn" (fun _ ->
            let actual =  getColumn<int> "Column1" transformedFrame 
            let expected =  series [ Key().addCol ("keys", "key1") => 1;Key().addCol ("keys", "key2") =>3]
            Expect.equal actual expected "Drafo.Core getColumn did not return the expected value"
        )

        testCase "indexWithColumnValue" (fun _ -> 
            let actual = indexWithColumnValues ["keys"] testFrame
            let expected = 
                frame [
                    ("Column1")=> series [Key().addCol ("keys", "key1") => 1;Key().addCol ("keys", "key2") =>3]
                    ("Column2")=> series [Key().addCol ("keys", "key1") => 10;Key().addCol ("keys", "key2") =>100]    
                ]
                |>Frame.addCol "keys"(series [Key().addCol ("keys", "key1") => "key1" ;Key().addCol ("keys", "key2") =>"key2"])
            Expect.equal  actual expected "Drafo.Core indexWithColumnValues did not return the expected value"   
        )

        testCase "readFrame" (fun _ -> 
            let actual = readFrame @"C:\Users\hochs\source\repos\Drafo\tests\Drafo.Tests\TestFour.csv"
            let expected =
                frame [
                    ("TestColOne")=> series [0 => "1";1 =>"3"]
                    ("TestColTwo")=> series [0 => "2";1 =>"4"]    
                ]
                |>Frame.addCol "keys"(series [0 => "key1" ;1=>"key2"])

            Expect.equal  actual expected "Drafo.Core readFrame did not return the expected value" 
        )

        testCase "readAndIndexFrame" (fun _-> 
            let actual = readAndIndexFrame ["keys"] @"C:\Users\hochs\source\repos\Drafo\tests\Drafo.Tests\TestFour.csv"
            let expected =
                frame [
                    ("TestColOne")=> series [0 => "1";1 =>"3"]
                    ("TestColTwo")=> series [0 => "2";1 =>"4"]    
                ]
                |>Frame.addCol "keys"(series [0 => "key1" ;1=>"key2"])
                |>indexWithColumnValues ["keys"]
            Expect.equal  actual expected "Drafo.Core readAndIndexFrame did not return the expected value" 
        )

        testCase "seriesToFrame" (fun _ ->
            let actual = series [ Key().addCol ("keys", "key1") => 1;Key().addCol ("keys", "key2") =>3]|>seriesToFrame
            let expected =
                frame [
                    ("keys")=> series [0 => "key1";1 =>"key2"]
                ]
                |>Frame.addCol "Value"(series [0 => 1 ;1=>3])

            Expect.equal  actual expected "Drafo.Core seriesToFrame did not return the expected value"
        )

        testCase "rowKeyToColumns" (fun _ ->
            let actual =
                frame [
                    ("Column1")=> series [Key().addCol ("keys", "key1") => 1;Key().addCol ("keys", "key2") =>3]
                    ("Column2")=> series [Key().addCol ("keys", "key1") => 10;Key().addCol ("keys", "key2") =>100]    
                ]
                |> rowKeyToColumns
            let expected = 
                frame [
                    ("keys")=> series [0 => "key1";1 =>"key2"]
                ]
                |>Frame.addCol "Column1"(series [0 => 1 ;1=>3])
                |>Frame.addCol "Column2"(series [0=> 10; 1=>100])
            Expect.equal  actual expected "Drafo.Core rowKeyToColumns did not return the expected value"
        )

        testCase "createFilter" (fun _ ->
            let filterOp (x:int)=
                if x= 1 then true else false
            let actual = 
                series [ Key().addCol ("keys", "key1") => 1;Key().addCol ("keys", "key2") =>3] 
                |> createFilter filterOp
            let expected = series [ Key().addCol ("keys", "key1") => true;Key().addCol ("keys", "key2") =>false] 
            Expect.equal  actual expected "Drafo.Core createFilter did not return the expected value"
        )

        testCase "transform"(fun _->
            let transformFuction a= 
                if a = 1 then 0
                else a
            let actual =
                series [ Key().addCol ("keys", "key1") => 1;Key().addCol ("keys", "key2") =>3] 
                |> transform transformFuction
            let expected = series [ Key().addCol ("keys", "key1") => 0;Key().addCol ("keys", "key2") =>3] 
            Expect.equal  actual expected "Drafo.Core transform did not return the expected value"
        )

        testCase "zip" (fun _ ->
            let actual = zip (fun x y -> x + y) (series [ Key().addCol ("keys", "key1") => 1;Key().addCol ("keys", "key2") =>3]) (series [ Key().addCol ("keys", "key1") => 6;Key().addCol ("keys", "key2") =>10]) 
            let expected =series [ Key().addCol ("keys", "key1") => 7;Key().addCol ("keys", "key2") =>13]
            Expect.equal  actual expected "Drafo.Core zip did not return the expected value"
        )

        testCase "dropAllKeyColumnsBut" (fun _ ->
            let actual = dropAllKeyColumnsBut ["keys"] ((Key().addCol ("keys", "key1")).addCol ("key", "key2"))
            let expected =  Key().addCol ("keys","key1")
            Expect.equal  actual expected "Drafo.Core dropAllColumnsBut did not return the expected value"
        )

        testCase "dropKeyColumns" (fun _ ->
            let actual = dropKeyColumns ["keys"] ((Key().addCol ("keys", "key1")).addCol ("key", "key2"))
            let expected =  Key().addCol ("key","key2")
            Expect.equal  actual expected "Drafo.Core dropColumns did not return the expected value"
        )

        testCase "groupTransform" (fun _ ->
            let op=
                (fun (x:seq<float>) -> 
                let m = Seq.mean x
                fun x -> x / m)
            let column= getColumn<float> "Column1" transformedFrame
            let actual = groupTransform op dropAllKeyColumnsBut ["Column1"] column
            
            let expected = series [ Key().addCol ("keys", "key1") => 0.5;Key().addCol ("keys", "key2") =>1.5]
            Expect.equal  actual expected "Drafo.Core groupTransform did not return the expected value"
        )

        testCase "createGroupFilter" (fun _ ->

            let actual = creatF 1. transformedFrame
            let expected = series [ Key().addCol ("keys", "key1") => true ;Key().addCol ("keys", "key2") =>true]
            Expect.equal  actual expected "Drafo.Core createFilter did not return the expected value"

        )

        testCase "aggregate" (fun _ ->
            let op =fun (x:seq<float>) -> Seq.mean x
            let column= getColumn<float> "Column1" transformedTestFrameTwo
            let actual = aggregate op dropAllKeyColumnsBut ["keys"]  [creatF 1. transformedTestFrameTwo] column
            let expected = series [ Key().addCol("keys", "key1") => 2.]
            Expect.equal  actual expected "Drafo.Core aggregate did not return the expected value"
        ) 

        testCase "pivot" (fun _ ->
            let actual =pivot "keys" transformedTestFrameTwo
            let expected = 
                frame [
                    ("key1.Column1")=> series [Key().addCol("KeyPartTwo","key2") => 1;Key().addCol("KeyPartTwo","key3")=> 3]
                    ("key1.Column2")=> series [Key().addCol("KeyPartTwo","key2") => 10;Key().addCol("KeyPartTwo","key3")=> 100]
                ]
                |>Frame.addCol "key1.keys"(series [Key().addCol("KeyPartTwo","key2") => "key1";Key().addCol("KeyPartTwo","key3")=> "key1"])
                |>Frame.addCol "key1.KeyPartTwo"(series [Key().addCol("KeyPartTwo","key2") => "key2";Key().addCol("KeyPartTwo","key3")=> "key3"])
                

            Expect.equal  actual expected "Drafo.Core pivot did not return the expected value"
            )
        

        testCase "assemble" (fun _ ->
            
            let actual = assemble ["Column1",series [ Key().addCol ("keys", "key1") => 0.5;Key().addCol ("keys", "key2") =>1.5]]
            let expected = 
                frame [
                ("Column1")=> series [ Key().addCol ("keys", "key1") => 0.5;Key().addCol ("keys", "key2") =>1.5]
                ]
            Expect.equal  actual expected "Drafo.Core assemble did not return the expected value"
        )
        testCase "GroupFilterSingle" (fun _ ->
            let actual =  groupFilter (Stdev 1.) transformedTestFrameTwo ["keys"] "Column1"
            let expected =  series [ (Key().addCol ("keys", "key1")).addCol("KeyPartTwo","key2") => true; (Key().addCol("keys", "key1")).addCol("KeyPartTwo","key3")  =>true]
            Expect.equal actual expected "groupFilter did not return the expected value"
        )
        testCase "GroupFilterMulti" (fun _ ->
            let actual =  groupFilterAllCol (Stdev 1.) TestFrameWithOutColumnsForIndexing ["keys"] |>Array.ofSeq
            let expected =  
                let sOne =series[(Key().addCol ("keys", "key1")).addCol("KeyPartTwo","key2") => true; (Key().addCol("keys", "key1")).addCol("KeyPartTwo","key3")  =>true]
                let sTwo =series[(Key().addCol ("keys", "key1")).addCol("KeyPartTwo","key2") => true; (Key().addCol("keys", "key1")).addCol("KeyPartTwo","key3")  =>true]
                let sCom = seq [sOne;sTwo]
                sCom|>Array.ofSeq
                
            Expect.equal actual expected "groupFilterAllCol did not return the expected value"
        )
        testCase "GroupWiseNumericTransformSingle" (fun _ ->
            let actual =  groupWiseNumericTransform SubstractMean ["keys"] TestFrameWithOutColumnsForIndexing "Column1"
            let expected =  series [ (Key().addCol ("keys", "key1")).addCol("KeyPartTwo","key2") => -1.; (Key().addCol("keys", "key1")).addCol("KeyPartTwo","key3")  =>1.]
            Expect.equal actual expected "GroupWiseNumericTransform did not return the expected value"
            )
        testCase "GroupWiseNumericTransformMulty" (fun _ ->
            let actual =  groupWiseNumericTransformAllCols SubstractMean ["keys"] TestFrameWithOutColumnsForIndexing 
            let expected =  
                frame [ 
                    ("Column1")=> series [(Key().addCol ("keys", "key1")).addCol("KeyPartTwo","key2") => -1.; (Key().addCol("keys", "key1")).addCol("KeyPartTwo","key3")  =>1.]
                    ("Column2")=> series [(Key().addCol ("keys", "key1")).addCol("KeyPartTwo","key2") => -45.; (Key().addCol("keys", "key1")).addCol("KeyPartTwo","key3")  =>45.]
                    ]
            Expect.equal actual expected "GroupWiseNumericTransformAllCols did not return the expected value"
            )
        testCase "NumericFilterSingle" (fun _ ->
            let actual =  numericFilter (IsBiggerThan 2.) transformedTestFrameTwo "Column1" 

            let expected =  series [ (Key().addCol ("keys", "key1")).addCol("KeyPartTwo","key2") => false; (Key().addCol("keys", "key1")).addCol("KeyPartTwo","key3")  =>true]
            Expect.equal actual expected "numericFilter did not return the expected value"
        )
        testCase "NumericFilterMulti" (fun _ ->
            let actual =  numericFilterServeralCol (IsBiggerThan 2.) TestFrameWithOutColumnsForIndexing |>Array.ofSeq
            let expected =  
                let sOne =series[(Key().addCol ("keys", "key1")).addCol("KeyPartTwo","key2") => false; (Key().addCol("keys", "key1")).addCol("KeyPartTwo","key3")  =>true]
                let sTwo =series[(Key().addCol ("keys", "key1")).addCol("KeyPartTwo","key2") => true; (Key().addCol("keys", "key1")).addCol("KeyPartTwo","key3")  =>true]
                let sCom = seq [sOne;sTwo]
                sCom|>Array.ofSeq
        
            Expect.equal actual expected "numericFilterServeralCol did not return the expected value"
        )
        testCase "NumericAggregationSingle" (fun _ ->

            let actual =  numAggregat Mean transformedTestFrameTwo "Column1" ["keys"] [filterForTests]

            let expected =  series[(Key().addCol ("keys", "key1")) => 3.]

            Expect.equal actual expected "numAggregat did not return the expected value"
        )
        testCase "NumericAggregationrMulti" (fun _ ->
            let actual =  numAgAllCol Mean TestFrameWithOutColumnsForIndexing ["keys"] seqOfFilterMulty
            let expected =  
                frame[
                    "Column1" => series [(Key().addCol ("keys", "key1")) => 3.]
                    "Column2" => series [(Key().addCol("keys", "key1"))  =>55.]
                ]

        
            Expect.equal actual expected "numAgAllCol did not return the expected value"
        )
        testCase "StringAggregationSingle" (fun _ ->
            let filterForTestsForStringAgg = seq [
                series[(Key().addCol ("keys", "key1")).addCol("KeyPartTwo","key2") => false; (Key().addCol("keys", "key1")).addCol("KeyPartTwo","key3")  =>true]

            ]
            let actual =  stAggregate  (Concat "Hallo") TestFrameWithOutColumnsForIndexingForStrinAg "keys" ["keys"] filterForTestsForStringAgg

            let expected =  series[(Key().addCol ("keys", "key1")) => "key1"]

            Expect.equal actual expected "stAggregate did not return the expected value"
        )
        testCase "StringAggregationrMulti" (fun _ ->
            let filterForTestsForStringAgg = seq [
                series[(Key().addCol ("keys", "key1")).addCol("KeyPartTwo","key2") => false; (Key().addCol("keys", "key1")).addCol("KeyPartTwo","key3")  =>true]
                series[(Key().addCol ("keys", "key1")).addCol("KeyPartTwo","key2") => true; (Key().addCol("keys", "key1")).addCol("KeyPartTwo","key3")  =>true]
            ]
            let filterForTestsForStringAggTwo = seq [
                series[(Key().addCol ("keys", "key1")).addCol("KeyPartTwo","key2") => false; (Key().addCol("keys", "key1")).addCol("KeyPartTwo","key3")  =>true]
                series[(Key().addCol ("keys", "key1")).addCol("KeyPartTwo","key2") => true; (Key().addCol("keys", "key1")).addCol("KeyPartTwo","key3")  =>true]
            ]
            let seqOfFilterMultyForString =
                filterForTestsForStringAgg
                |>Seq.mapi (fun i x -> 
                    let colKeysForSeq seqIt b= 
                        seqIt
                        |>Seq.item b
                    let cIII = seq [x;colKeysForSeq filterForTestsForStringAggTwo i]
                    cIII)
            let actual =  stAggregateFullFrame (Concat "Hallo") TestFrameWithOutColumnsForIndexingForStrinAg ["keys"] seqOfFilterMultyForString
            let expected =  
                frame[
                    "keys" => series [(Key().addCol ("keys", "key1")) => "key1"]
                    "KeyPartTwo" => series [(Key().addCol("keys", "key1"))  =>"key2Hallokey3"]
                ]

        
            Expect.equal actual expected "stAggregateFullFrame did not return the expected value"
        )
        testCase "NumericAggregationSingleWithOutFilter" (fun _ ->

            let actual =  numAggregat Mean transformedTestFrameTwo "Column1" ["keys"] []

            let expected =  series[(Key().addCol ("keys", "key1")) => 2.]

            Expect.equal actual expected "the function numAggregat without filter did not return the expected value"
        )
        testCase "Pipeline" (fun _ ->

           
            let readIn = 
                readFrame @"C:\Users\hochs\source\repos\Drafo\tests\Drafo.Tests\TestFour.csv"
                |>Frame.addCol "KeyPartTwo"(series [0=> "key1" ;1 =>"key1"])
                
            let indexWCol = indexWithColumnValues (seq ["keys";"KeyPartTwo"]) readIn
                
            let filterForPipeline =  numericFilter (IsBiggerThan 1.) indexWCol "TestColTwo" 
                
            let aggregationStep =  
                numAggregat Mean indexWCol "TestColTwo" ["KeyPartTwo"] [filterForPipeline]
                |>seriesToFrame
                
            let savedFrame = aggregationStep.SaveCsv (@"C:\Users\hochs\source\repos\Drafo\tests\Drafo.Tests\TestSavedOutput.csv",includeRowKeys=false,separator='\t')
            let readInSavedFrame= readFrame(@"C:\Users\hochs\source\repos\Drafo\tests\Drafo.Tests\TestSavedOutput.csv")
            let _ = readInSavedFrame.DropColumn "KeyPartTwo"
            
            let expected = 
                frame [
                    "Value"=> series [0 => "3"]
                ]
            Expect.equal readInSavedFrame expected "The pipeline test did not return the expected value"

        )
        testCase "PipelineTwoAggregations" (fun _ ->
        
            let frameWithColsForThreeKeyLevels = 
                frame [
                    ("Column1")=> series ["row1" => 4;"row2" =>3;"row3" =>3;"row4" =>3]
                    ("Column2")=> series ["row1" => 110;"row2" =>100;"row3" =>3;"row4" =>3]    
                ]
                |>Frame.addCol "Gen"(series ["row1" => "A" ;"row2" =>"A";"row3" =>"A";"row4" =>"A"])
                |>Frame.addCol "BiologicalR"(series ["row1" => "BioRepA" ;"row2" =>"BioRepA";"row3" =>"BioRepB";"row4" =>"BioRepB"])
                |>Frame.addCol "TecknicalR"(series ["row1" => "RepA" ;"row2" =>"RepB";"row3" =>"RepA";"row4" =>"RepB"])

            //frameWithColsForThreeKeyLevels

            let indexWCol = 
                indexWithColumnValues (seq ["Gen";"BiologicalR";"TecknicalR"]) frameWithColsForThreeKeyLevels
                |> Frame.dropCol "Gen"
                |> Frame.dropCol "BiologicalR"
                |> Frame.dropCol "TecknicalR"


            let groupFilterForPipeline = groupFilterAllCol (Stdev 1.) indexWCol (seq ["Gen";"BiologicalR"])


            let numFilForPipeline =numericFilterServeralCol (IsBiggerThan 2.) indexWCol 
            let seqOfFilterMultyForString =
                groupFilterForPipeline
                |>Seq.mapi (fun i x -> 
                    let colKeysForSeq seqIt b= 
                        seqIt
                        |>Seq.item b
                    let cIII = seq [x;colKeysForSeq numFilForPipeline i]
                    cIII)


            let aggregationOneMeanTR = numAgAllCol Mean indexWCol  (seq ["Gen";"BiologicalR"]) seqOfFilterMultyForString // seqOfFilterMultyForString

            let filterSecAggre = 
                numericFilterServeralCol (IsBiggerThan 2.) aggregationOneMeanTR
                |>Seq.map (fun x -> seq [x])
            let aggregationOneMeanBR = numAgAllCol Mean aggregationOneMeanTR (seq ["Gen"]) filterSecAggre

                    
            let expected = 
                frame [
                    "Column1"=> series [Key().addCol ("Gen", "A") => 3.25]
                    "Column2"=> series [Key().addCol ("Gen", "A") => 54.]
                ]
            Expect.equal aggregationOneMeanBR expected "Pipeline with two Aggregations did not return the expected value"
        
                )

    ]
