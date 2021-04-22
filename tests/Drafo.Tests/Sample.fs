module Tests

open Expecto
open Drafo
open Deedle
open System.Diagnostics


[<Tests>]
let tests =
  testList "Core" [
    let testFrame = 
      let index1 = 
        Series.ofValues [
          "one"
          "two"
          "three"
          "four"    
        ]
      let index2 = 
        Series.ofValues [
          1
          2
          3
          4    
        ]
      let index3 = 
        Series.ofValues [
          "group 1"
          "group 1"
          "group 2"
          "group 2"    
        ]
      let value1 = 
        Series.ofValues [
          1000.
          2000.
          nan
          1.    
        ]
      let value2 = 
        Series.ofValues [
          10000.
          20000.
          nan
          10.    
        ]  
      Frame.ofColumns(["index1",index1])
      |> Frame.addCol "index2" index2
      |> Frame.addCol "index3" index3
      |> Frame.addCol "value1" value1
      |> Frame.addCol "value2" value2

    let testFrameIndexed = 
      testFrame
      |> Drafo.Core.indexWithColumnValues ["index1";"index2";"index3"]
    
    testCase "indexing from column key list :1" <| fun _ ->
      let k = testFrameIndexed.GetRowKeyAt (int64 1)
      let i = k?index1 |> string 
      Expect.equal i "two" "value sucessfully added as key property"

    testCase "indexing from column key list :2" <| fun _ ->
      let k = testFrameIndexed.GetRowKeyAt (int64 1)
      let i = k?index2 |> string  |> int 
      Expect.equal i 2 "value sucessfully added as key property"
    
    testCase "indexing from column key list :3" <| fun _ ->
      let k = testFrameIndexed.GetRowKeyAt (int64 1)
      let i = k?index3 |> string  
      Expect.equal i "group 1" "value sucessfully added as key property"  
    
    testCase "getColumn" <| fun _ ->
      let c =
        testFrameIndexed
        |> Drafo.Core.getColumn<float>"value1"
      let check = Series.getAt 1 c = 2000.
      Expect.isTrue check "columnKeysAreEqual" 

    testCase "createFilter" <| fun _ ->
      let c =
        testFrameIndexed
        |> Drafo.Core.getColumn<string>"index3"
      let f = Drafo.Core.createFilter (fun v -> v = "group 1") c
      let checkTrue = Series.getAt 1 f 
      let checkFalse = Series.getAt 2 f |> not
      Expect.isTrue (checkTrue && checkFalse) "columnKeysAreEqual" 
    
    testCase "aggregate_simple" <| fun _ ->
      let v = 
        testFrameIndexed
        |> Drafo.Core.getColumn<float>"value1"
      let a = Drafo.Core.aggregate (Seq.fold (fun acc x -> acc + x) 0.) ["index3"] [] v 
      let checkTrue = Series.getAt 0 a |> (=) 3000. 
      let checkTrue' = Series.getAt 1 a |> (=) 1.
      Expect.isTrue (checkTrue && checkTrue') "columnKeysAreEqual" 
   
    testCase "aggregate_withFilter" <| fun _ ->
      let c =
        testFrameIndexed
        |> Drafo.Core.getColumn<string>"index3"
      let f = Drafo.Core.createFilter (fun v -> v = "group 1") c
      let v = 
        testFrameIndexed
        |> Drafo.Core.getColumn<float>"value1"
      let a = Drafo.Core.aggregate (Seq.fold (fun acc x -> acc + x) 0.) ["index3"] [f] v 
      let checkTrue = Series.getAt 0 a |> (=) 3000. 
      let checkTrue' = Series.countValues a |> (=) 1
      Expect.isTrue (checkTrue && checkTrue') "columnKeysAreEqual" 

    testCase "assemble_keyEqual" <| fun _ ->
      let c =
        testFrameIndexed
        |> Drafo.Core.getColumn<string>"index3"
      let f = Drafo.Core.createFilter (fun v -> v = "group 1") c
      let v1 = 
        testFrameIndexed
        |> Drafo.Core.getColumn<float>"value1"
      let a1 = Drafo.Core.aggregate (Seq.fold (fun acc x -> acc + x) 0.) ["index3"] [f] v1 
      let v2 = 
        testFrameIndexed
        |> Drafo.Core.getColumn<float>"value2"
      let a2 = Drafo.Core.aggregate (Seq.fold (fun acc x -> acc + x) 0.) ["index3"] [f] v2 
      let assembeled = 
        Drafo.Core.assemble 
            [
            "V1"  , a1 :> ISeries<Drafo.Core.Key>
            "V2"  , a2 :> ISeries<Drafo.Core.Key>
            ]
      let toSave = 
        assembeled
      let ckEqual = 
        Seq.map2 (fun x y -> x = y) toSave.ColumnKeys ["V1";"V2"]
        |> Seq.contains false
        |> not
      Expect.isTrue (ckEqual) "columnKeysAreEqual"    

    testCase "assemble_keyEqual_2" <| fun _ ->
      let c =
        testFrameIndexed
        |> Drafo.Core.getColumn<string>"index3"
      let f = Drafo.Core.createFilter (fun v -> v = "group 1") c
      let v1 = 
        testFrameIndexed
        |> Drafo.Core.getColumn<float>"value1"
      let a1 = Drafo.Core.aggregate (Seq.fold (fun acc x -> acc + x) 0.) ["index3"] [f] v1 
      let v2 = 
        testFrameIndexed
        |> Drafo.Core.getColumn<float>"value2"
      let a2 = Drafo.Core.aggregate (Seq.fold (fun acc x -> acc + x) 0.) ["index3"] [f] v2 
      let assembeled = 
        Drafo.Core.assemble 
            [
            "V1"  , a1 :> ISeries<Drafo.Core.Key>
            "V2"  , a2 :> ISeries<Drafo.Core.Key>
            ]
      let toSave = 
        assembeled
        |> Drafo.Core.rowKeyToColumns 
      let ckEqual = 
        Seq.map2 (fun x y -> x = y) toSave.ColumnKeys ["index3";"V1";"V2"]
        |> Seq.contains false
        |> not
      Expect.isTrue (ckEqual) "columnKeysAreEqual"                                  
  
    testCase "getColumnConsole_help" <| fun _ ->
      let path = @"C:\Users\David Zimmer\source\repos\Drafo\bin\GetColumn\net5.0\GetColumn.exe"// 
      printfn "Path: %s" path
      let arg = "--help"
      let p =                        
        new ProcessStartInfo
          (FileName = path, UseShellExecute = false, Arguments = arg, 
           RedirectStandardError = false, CreateNoWindow = true, 
           RedirectStandardOutput = false, RedirectStandardInput = true) 
        |> Process.Start
      p.WaitForExit()
      let c = p.ExitCode
      p.Close()
      Expect.equal c 0 "Ran." 

  
  ]
