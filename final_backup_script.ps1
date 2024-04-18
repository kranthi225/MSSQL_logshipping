
param([string]$Backuptype,[string]$S3BucketName,[string]$source_path,[string]$filename,[string]$restore_db_name,[string]$Target_Server,[string]$Target_Username,[string]$Target_Password)
#$source = C:\Program Files\Microsoft SQL Server\MSSQL13.MSSQLSERVER\MSSQL\Backup\'
$RunId = [guid]::NewGuid()
#$Global:S3BucketName = "inmarsat-$RunId"
#$Global:S3BucketName ="inmarsat-82af4b55-4526-4992-9e18-a4644fb42e9e"


function Execute-SqlQuery-Target
(
[string]$Server, 
[string]$Database1, 
[string]$Username, 
[string]$Password,
#[Bool]$UseWindowsAuthentication = $False, 
[string]$Query2, 
[int]$CommandTimeout=0
 )
{
 #Create Connection string
 $ConnectionString = "Server=$Server; Database=$Database; User ID=$username; Password=$password;"
 #If ($UseWindowsAuthentication) { $ConnectionString += "Trusted_Connection=Yes; Integrated Security=SSPI;" } else { $ConnectionString += "User ID=$username; Password=$password;" }
 
 #Connect to database
 $Connection = New-Object System.Data.SqlClient.SqlConnection($ConnectionString);
 Write-Host "conn : $ConnectionString"
 $Connection.Open();

 #Create query object
 $Command = $Connection.CreateCommand();
 $Command.CommandText = $Query2;
 $Command.CommandTimeout = $CommandTimeout;

 #Exucute query
 $SqlDataAdapter = New-Object System.Data.SqlClient.SqlDataAdapter $Command;
 $DataSet = New-Object System.Data.DataSet;
 $SqlDataAdapter.Fill($DataSet) | Out-Null;

 #Return result
 If ($DataSet.Tables[0] -ne $Null) { $Table = $DataSet.Tables[0] }
 ElseIf ($table.Rows.Count -eq 0) { $Table = New-Object System.Collections.ArrayList }
 $Connection.Close();
 return $Table;
}

if ($Backuptype -eq "F")
{

   Set-Location $source_path
Write-Host "the source is $source_path"
$files = Get-ChildItem "$filename" | Select-Object -Property Name
try {
   if(Test-S3Bucket -BucketName $S3BucketName) {
      foreach($file in $files) {
         if(!(Get-S3Object -BucketName $S3BucketName -Key $file.Name)) { ## verify if exist
            Write-Host "Copying file : $file "
            Write-S3Object -BucketName $S3BucketName -File $file.Name -Key $file.Name -CannedACLName private 
            $Database1 = "master"
       $Query2 = "exec msdb.dbo.rds_restore_database @restore_db_name='$restore_db_name',@s3_arn_to_restore_from='arn:aws:s3:::$($S3BucketName)/$($filename)'
       ,@with_norecovery=1;"
       $QueryResult = Execute-SqlQuery-Target -Server $Target_Server -Database1 $Database1 -Username $Target_Username -Password $Target_Password -Query2 $Query2;
            
         } 
      }
   } Else {
      Write-Host "The bucket $bucket does not exist."
   }
} catch {
   Write-Host "Error uploading file $file"
}

}

Else {

Set-Location $source_path
$files = Get-ChildItem '*.trn' | sort LastWriteTime | Select-Object -Property Name
try {
   if(Test-S3Bucket -BucketName $S3BucketName) {
      foreach($file in $files) {
         if(!(Get-S3Object -BucketName $S3BucketName -Key $file.Name)) { ## verify if exist
            Write-Host "Copying file : $file "
            Write-S3Object -BucketName $S3BucketName -File $file.Name -Key $file.Name -CannedACLName private 
          
       $trim="$file".TrimStart(“File @{Name”)
       $trim2="$trim".TrimStart("=")
       $finaltrim="$trim2".TrimEnd("}")
       $Database1 = "master"
       $Query2 = "exec msdb.dbo.rds_restore_log @restore_db_name='$restore_db_name',@s3_arn_to_restore_from='arn:aws:s3:::$($S3BucketName)/$($finaltrim)'
       ,@with_norecovery=1;"


       $QueryResult = Execute-SqlQuery-Target -Server $Target_Server -Database1 $Database1 -Username $Target_Username -Password $Target_Password -Query2 $Query2;
      
       Write-Host "executable query is $Query2"
          
         } 
      }
   } Else {
      Write-Host "The bucket $bucket does not exist."
   }
} catch {
   Write-Host "Error uploading file $file"
}
}



#./final_backup_script.ps1 -Backuptype 'F' -S3BucketName inmarsat-82af4b55-4526-4992-9e18-a4644fb42e9e -source_path 'C:\Program Files\Microsoft SQL Server\MSSQL13.MSSQLSERVER\MSSQL\Backup\final_logshipping\' -filename final_logshipping.bak -restore_db_name final_logshipping246 -Target_Server sqlserver.cmha3vurzm78.us-east-1.rds.amazonaws.com -Target_Username sqlserver -Target_Password sqlserver



