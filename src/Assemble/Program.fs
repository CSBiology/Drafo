namespace Assemble

open System

open Argu
open CLIArgumentParsing
open System
open System.IO
open System.Reflection

module console = 

    let getRelativePath (reference: string) (path: string) =
                Path.Combine(reference, path)

    [<EntryPoint>]
    let main argv =
        let errorHandler = ProcessExiter(colorizer = function ErrorCode.HelpText -> None | _ -> Some ConsoleColor.Red)
        let parser = ArgumentParser.Create<CLIArguments>(programName =  (System.Reflection.Assembly.GetExecutingAssembly().GetName().Name),errorHandler=errorHandler)       
        let directory :string = System.IO.Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location)
        let getPathRelativeToDir = getRelativePath directory
        let results = parser.Parse(argv)
        let i = results.GetResult InputFolder      |> getPathRelativeToDir
        let o = results.GetResult OutputDirectory  |> getPathRelativeToDir
        let kc = results.GetResult KeyColumns       
        let tc = results.GetResult TargetCol       
        GetColumn.getColumn kc tc o i 
        0 // return an integer exit code