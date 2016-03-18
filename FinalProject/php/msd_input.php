<? php

 $outputdir = rand(0, 10000);
 $region    = $_POST['region'];

 //echo $region;
 //echo " ";
 $tempo = $_POST['tempo'];

 //echo $tempo;
 //echo " ";
 $mode = $_POST['mode'];

 //echo $mode;

 $args = implode(" ", $_POST);

 //echo $args;
 echo "<br>";
 echo "<hr>";

 $songlist = "demoListClassicSlowEm";

 switch ($_POST['runas'])
 {
 case "demo":

     //echo "demo";
     $songlist = "demoListContSlowMinE";
     break;

 case "partial":

     //echo "partial";
     $songlist = "demoListContAllMajG";
     break;

 case "full":

     //echo "full";
     exec("java -jar /home/anuj/work/MSD/AWS2.jar $args $outputdir");
     $songlist = "songList".$outputdir;
     break;
 }

 echo "<table cellspacing=\"10\">";
 $handle = fopen($songlist, "r");

 if ($handle)
 {
     echo "<tr><td>";
     echo "<p>Hotness</p>";
     echo "</td>";
     echo "<td>";
     echo "<p>Danceability</p>";
     echo "</td>";
     echo "<td>";
     echo "<p>Song Name</p>";
     echo "</td>";
     echo "<td>";
     echo "<p>Artist Name</p>";
     echo "</td>";
     echo "<td>";
     echo "<p>Year</p>";
     echo "</td></tr>";

     while (($line = fgets($handle)) != = false)
     {
         echo "<tr>";
         $songinfo = explode("\t", $line);
         echo "<td>";
         echo $songinfo[0];
         echo "</td>";
         echo "<td>";
         echo $songinfo[4];
         echo "</td>";
         echo "<td>";
         echo $songinfo[1];
         echo "</td>";
         echo "<td>";
         echo $songinfo[2];
         echo "</td>";
         echo "<td>";
         echo $songinfo[3];
         echo "</td>";
     }

     echo "</tr>";
 }
 else
 {
     echo "no songs found";
 }

 fclose($handle);
 echo "</table>";

 ?>
