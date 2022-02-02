import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.sql.SQLContext

import java.util.Scanner
import java.sql.DriverManager
import java.sql.Connection
import com.mysql.cj.xdevapi.UpdateStatement
import java.io.File
import java.io.PrintWriter



//?for ";" in hive '\u0059'

//? SHIFT + ALT + F => to auto format

//? sc.nextLine().trim.toInt





object Project_1_Sharks {

    def main(args: Array[String]): Unit = {
         // declaring all variables needed for program
        var scanner = new Scanner(System.in)
        val log = new PrintWriter(new File("Sharks.log"))
        
        var userName = ""
        var userPassword = ""
        var userPassword2 = ""
        var userNameUpdate = ""
        var userPasswordUpdate = ""
        var userPasswordUpdate2 = ""
        var userAdmin = ""
        var adminPassword = ""
        var adminPassword2 = ""
        var choice = ""
        var choice2 = ""
        var userMenu = ""
        var shark = true
        
        


        // This block of code is need to connect to spark/hive/hadoop
        System.setSecurityManager(null)
        System.setProperty("hadoop.home.dir", "C:\\hadoop\\")
        val conf = new SparkConf()
            .setMaster("local")
            .setAppName("Project_1_Sharks")
        val sc = new SparkContext(conf)
        sc.setLogLevel("ERROR")
        val hiveCtx = new HiveContext(sc)
        import hiveCtx.implicits._


        // make the connection to mySQL
        val driver = "com.mysql.jdbc.Driver"
        // Modify for whatever port you are running your DB on
        val url = "jdbc:mysql://localhost:3306/Project_1_Sharks"
        val username = "root"
        //? DON'T FORGET TO DELETE PASSWORD BEFORE PUSHING TO GITHUB
        val password = "################" // Update to include your password
        var connection:Connection = null 
        //val statement = connection.createStatement()
        
        
        
        
        





         
        try{
            // make the connection
            Class.forName(driver)
            connection = DriverManager.getConnection(url, username, password)  
            val statement = connection.createStatement() 


            // Welcome screen to the app
            println("                                                                                                         ")
            println("                                  *                                                                      ") 
            println("                                *   *                                                                    ")
            println("                               *     *                                   *                               ")
            println("                              *       *                                 * *                              ")
            println("                             *         *                               *   *                             ")
            println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~*~~~~~~~~~~~*~~~~~~~~~~~~~~~~~~~~~~~~~~~~~*~~~~~*~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
            println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
            println("                                      WELCOME TO THE SHARK ATTACK DATABASE                               ")
            println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
            println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
            println("")        
            println("")


                  
                // Application loop
                while (shark){
                    mainMenu()

                    // Start of program
                    def mainMenu(){
                        println("###################################")
                        println("Please choose from the menu below: ")
                        println("(1) Create Account ")
                        println("(2) User Log In ")
                        println("(3) Admin Log In ")
                        println("(0) Quit Program")
                        println("###################################")
                        var choice= (scanner.nextInt())
                        scanner.nextLine()
                      

                        if (choice == 1){
                            createAccount()
                                
                        }else if(choice == 2){
                            userLogIn()

                        }else if(choice == 3){
                            adminLogIn()

                        }else if(choice == 0){
                            exitProgram()
                                
                        }else if(( choice != 0 || choice != 1|| choice != 1 || choice != 2 || choice != 3 )){
                            println("Not a valid choice. Try again")
                            //mainMenu()
                        }
                                
                    }
                        
                    // Create User Account    
                    def createAccount(): Unit = {
                        println("Please type your username")
                        userName = (scanner.nextLine())
                        println("")
                        println("Please type your password")
                        userPassword = (scanner.nextLine())
                        println("")
                        println("Please type your retype password")
                        userPassword2 = (scanner.nextLine())
                        if (userPassword == userPassword2){
                            println(" Account has been created")
                            println("")
                            userMenu()

                        }else if (userPassword != userPassword2){
                            println(" Passwords do not match, please try again")
                            println("")
                            println("Please type your password!!")
                            userPassword = (scanner.nextLine())
                            println("")
                            println("Please type your retype password!!")
                            userPassword2 = (scanner.nextLine())
                            println(" Account has been created!!!")
                            println("")
                            userMenu()

                        }else if (userPassword == null){
                            println(" Password Cannot Be Blank")
                            println("")
                            println("Please type your password!!")
                            userPassword = (scanner.nextLine())
                            println("")
                            println("Please type your retype password!!")
                            userPassword2 = (scanner.nextLine())
                            println(" Account has been created!!!")
                            println("")
                            userMenu()
                        }
                        //Updating table with userName and password after creating a new account
                        val resultSet2 = statement.executeUpdate("INSERT INTO Sharks (userName, userPassword, userPassword2) VALUES ('"+userName+"', '"+userPassword+"',  '"+userPassword2+"');")
                        log.write("Executing 'INSERT INTO Sharks (userName, userPassword, userPassword2) VALUES ('"+userName+"', '"+userPassword+"',  '"+userPassword2+"');\n")

                    } 

                    //User logging in
                    def userLogIn(){
                        println(" Please type a User Name")
                        userName = scanner.nextLine().trim()
                        println("")
                        println(" Please type A Password")
                        userPassword = scanner.nextLine().trim()
                        val resultSet = statement.executeQuery("SELECT COUNT(*) FROM userAccount WHERE userName='"+userName+"' AND userPassword='"+userPassword+"';")
                        log.write("Executing 'SELECT COUNT(*) FROM userAccount WHERE userName='"+userName+"' AND userPassword='"+userPassword+"');\n")
                        while ( resultSet.next() ) {
                            if (resultSet.getString(1) == "1") {
                                println("You Have Logged In Successfully")
                                userChoice()
                                
                            }else{
                                println("Username/password combo not found. Try again!")
                                userLogIn()
                                                    
                            }
                        }
                        
                                                    
                    }    


                    def userChoice(){
                        // option for user to choose query menu or user data menu
                        println(" Welcome User. Please choose from below: ")
                        println("################################")
                        println(" (1) User Query menu")
                        println("")
                        println(" (2) User Information Menu")
                        println("")
                        println(" (3) Main menu")
                        println("")
                        println(" (4) Exit the program")
                        println("####################################")
                        var choice2 =  (scanner.nextInt())
                        (scanner.nextLine()) 
                        if (choice2 == 1){
                            println(" User Query Menu...")
                            userMenu()

                            
                        }else if (choice2 == 2) {
                            println(" User Information Menu")
                            userInformationOption()
                            
                        }else if (choice2 == 3) {
                            println(" Main Menu")
                            mainMenu()
                            
                        }else if (choice2 == 4) {
                            println(" Exit Program")
                            exitProgram()
                            
                            
                        }else if ((choice != 1 || choice != 2 || choice != 3 || choice != 4)) {
                            println(" Not a valid choice, please try again!!!")
                            userChoice()
                            
                        }

                    }      




                    def userInformationOption(){
                        println("")
                        println("################################")
                        println(" (1) Update User Name ")
                        println("")
                        println(" (2) Update User Password")
                        println("")
                        println(" (3) Previous Menu ")
                        println("")
                        println(" (0) Exit the program ")
                        println("")
                        println("#################################")
                        var choice2 =  (scanner.nextInt())
                        (scanner.nextLine()) 
                        if (choice2 == 1){
                            println(" Type A New User Name")
                            userNameUpdate =  scanner.nextLine().trim()
                            val resultSet2_1 = statement.executeUpdate("UPDATE userAccount SET userName = ('"+userNameUpdate+"') WHERE userName = ('"+userName+"');")
                            log.write("UPDATE userAccount SET userName = ('"+userNameUpdate+"') WHERE username = ('"+userName+"') \n")
                            println("User Name Updated")
                            userChoice()

                        }else if (choice2 == 2){
                            println("Type User Name")
                            userName =  scanner.nextLine().trim()
                            println(" Type A New Password")
                            userPasswordUpdate =  scanner.nextLine().trim()
                            val resultSet2_2 = statement.executeUpdate("UPDATE userAccount SET userPassword = ('"+userPasswordUpdate+"') WHERE userName = ('"+userName+"');")
                            log.write("UPDATE userAccount SET userPassword = ('"+userPasswordUpdate+"') WHERE username = ('"+userName+"') \n")
                            println(" ReType Password")
                            userPasswordUpdate2 =  scanner.nextLine().trim()
                            val resultSet2_3 = statement.executeUpdate("UPDATE userAccount SET userPassword2 = ('"+userPasswordUpdate2+"') WHERE userName = ('"+userName+"');")
                            log.write("UPDATE userAccount SET userPassword2 = ('"+userPasswordUpdate2+"') WHERE username = ('"+userName+"') \n")
                            println("User Name Updated")
                            userChoice()

                        }else if (choice2 == 3){
                            userChoice()

                        }else if (choice2 == 0){
                            exitProgram()
                        
                        }else if (( choice != 0 || choice != 1 || choice != 2 || choice != 3)) {
                            println(" Not a valid choice, please try again!!!")
                            userChoice()
                        }
                    }



                    // User Menu
                    def userMenu(){
                        sharkAttackData()
                        println(" What type of data would you like to view. Please select below: ")
                        println("")
                        println(" (1) What is the total number of shark attacks recorded?")
                        println("")
                        println(" (2) What shark is responsible for the most shark attacks?")
                        println("")
                        println(" (3) Where do the most shark attacks happen?")
                        println("")
                        println(" (4) What time of day do most shark attacks happen?")
                        println("")
                        println(" (5) Are most shark attacks provoked or unprovoked?")
                        println("")
                        println(" (6) What is the age range of people attacked by sharks?")
                        println("")
                        println(" (0) To exit the program")
                        println("")
                        var choice2 =  (scanner.nextInt())
                        (scanner.nextLine()) 
                        if (choice2 == 1){
                            println(" What is the total number of shark attacks recorded?")
                            totalSharkAttacks()
                            userMenu()

                            
                        }else if (choice2 == 2) {
                            println(" What shark is responsible for the most shark attacks?")
                            sharkResponsible()
                            userMenu()
                            
                        }else if (choice2 == 3) {
                            println(" Where do the most shark attacks happen?")
                            locationMostSharkAttacks()
                            userMenu()
                            
                        }else if (choice2 == 4) {
                            println(" What time of day do most shark attacks happen?")
                            timeOfDaySharkAttack()
                            userMenu()
                            
                        }else if (choice2 == 5) {
                            println(" Are most shark attacks provoked or unprovoked?")
                            provokedUnprovokedAttacks()
                            userMenu()
                            
                        }else if (choice2 == 6) {
                            println(" What is the age range of people attacked by sharks?")
                            ageRangePeopleAttacked()
                            userMenu()
                        

                        }else if (choice2 == 7) {
                        println(" Return To Log In Screen?")
                        mainMenu()

                        }else if (choice2 == 0) {
                            exitProgram()
                            
                        }else if (( choice != 0 || choice != 1 || choice != 2 || choice != 3 || choice != 4 || choice != 5|| choice != 6 || choice != 7)) {
                            println(" Not a valid choice, please try again!!!")
                            userMenu()
                            
                        }
                    }

                    
                    
                    // logging in as Admin
                    def adminLogIn(){
                        //val statement2 = connection.createStatement()
                        println("Type Admin UserName")
                        userAdmin = scanner.nextLine().trim()
                        println("")
                        println("Type Admin Password")
                        adminPassword = scanner.nextLine().trim()
                        println("")     
                        val resultSet2 = statement.executeQuery("SELECT COUNT(*) FROM adminAccount WHERE userAdmin='"+userAdmin+"' AND adminPassword='"+adminPassword+"';")
                        log.write("Executing 'SELECT COUNT(*) FROM adminAccount WHERE userAdmin='"+userAdmin+"' AND adminPassword='"+adminPassword+"');\n")
                        while ( resultSet2.next() ) {
                            if (resultSet2.getString(1) == "1") {
                                println(" Log In Successful!!")
                                adminMenu()
                            }else{
                                println("Username/password combo not found. Try again!")
                                println("")
                                adminLogIn()
                                                    
                            }           
                        }
                    }

                    // Admin menu options
                    def adminMenu(){
                        println("")
                        println(" Welcome Admin. Please choose from below: ")
                        println("################################")
                        println(" (1) Update dataset ")
                        println("")
                        println(" (2) Delete a User from the Database")
                        println("")
                        println(" (3) Exit the program")
                        println("####################################")
                        var choice2 =  (scanner.nextInt())
                        (scanner.nextLine()) 
                        if (choice2 == 1){
                            println(" choice 1")
                            updateDataset()                            
                        
                        }else if (choice2 == 2) {
                            println(" choice 2.")
                            deleteUser()
                        
                        }else if (choice2 == 3) {
                            println(" choice 3.")
                            exitProgram()
                        
                        }else if (( choice != 1 || choice != 2 || choice != 3)) {
                            println(" Not a valid choice, please try again!!!")
                            adminMenu()
                        
                        }
                    }

                    // Update the data for user options 
                    def updateDataset(){
                        println("")
                        println(" Please choose from below ")
                        println("############################")
                        println(" (1) Update Data")
                        println("")
                        println(" (2) Return to Admin Menu ")
                        println("")
                        println(" (3) Exit program ")
                        println("#############################")
                        println("")
                        
                        var choice2 =  (scanner.nextInt())
                        (scanner.nextLine()) 
                        if (choice2 == 1){
                            println(" Updating Data on file ....")
                            sharkAttackData()                          
                        
                        }else if (choice2 == 2) {
                            adminMenu()
                        
                        }else if (choice2 == 3) {
                            println("Exiting Program.....")
                            exitProgram()

                        }else if (( choice != 1 || choice != 2 || choice !=3 )) {
                            println(" Not a valid choice, please try again!!!")
                            adminMenu()
                        
                        }
                    }


                    // DELETE USER 
                    def deleteUser(){
                        println("")
                        println("################################")
                        println(" (1) Choose A User To Delete ")
                        println("")
                        println(" (2) Exit the program")
                        println("")
                        println("#################################")
                        var choice2 =  (scanner.nextInt())
                        (scanner.nextLine()) 
                        if (choice2 == 1){
                            val resultSet4 = statement.executeQuery("SELECT * FROM userAccount")
                            log.write("Executing 'SELECT * FROM userAccount';\n")
                            println(" Type A User's Name")
                            userName =  scanner.nextLine().trim()
                            val resultSet3 =  statement.executeUpdate("DELETE FROM userAccount WHERE userName = ('"+userName+"');")
                            log.write("Executing 'DELETE User from database' \n")
                            println("User Deleted")
                            val resultSet5 = statement.executeQuery("SELECT * FROM userAccount")
                            log.write("Executing 'SELECT * FROM userAccount';\n")
                            adminMenu()


                        }else if (choice2 == 2) {
                            println(" choice 2.")
                            exitProgram()
                        
                        }else if (( choice != 1 || choice != 2 )) {
                            println(" Not a valid choice, please try again!!!")
                            deleteUser()
                        
                        }
                    }


                    // Exit program
                    def exitProgram(){
                        println("")
                        println("                                                                                                         ")
                        println("                                  *                                                                      ") 
                        println("                                *   *                                                                    ")
                        println("                               *     *                                   *                               ")
                        println("                              *       *                                 * *                              ")
                        println("                             *         *                               *   *                             ")
                        println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~*~~~~~~~~~~~*~~~~~~~~~~~~~~~~~~~~~~~~~~~~~*~~~~~*~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
                        println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
                        println("                             THANK YOU FOR VISITING THE SHARK ATTACK DATABASE                            ")
                        println("                                       DO STAY SAFE IN THEIR OCEANS                                      ")
                        println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
                        println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
                        println("")        
                        println("")
                        println("")
                        shark = false
                        
                    }

                    // Creates Shark Attack Database
                    def sharkAttackData(){
                        val output = hiveCtx.read
                            .format("csv")
                            .option("inferSchema", "true")
                            .option("header", "true")
                            .load("input/GSAF5 (1_2).csv")
                                    
                        //output.limit(20).show() // will print out the first 20 lines


                        // This code will create a temp view of the dataset you used and load the data into a permanent table
                        // inside of Hadoop. this will persist the data and only require this code to run once.
                        // After initialization this code will and creation of output will not me necessary
                        output.createOrReplaceTempView("temp_data")
                        hiveCtx.sql("SET hive.exec.dynamic.partition.mode=nonstrict")
                        hiveCtx.sql("SET hive.enforce.bucketing=false")
                        hiveCtx.sql("SET hive.enforce.sorting=false")
                        //hiveCtx.sql("USE project1_hive_scala")
                        hiveCtx.sql("CREATE TABLE IF NOT EXISTS shark1 (caseNumber STRING, date STRING, year INT, type STRING, country STRING, area STRING, location STRING, activity STRING, name STRING, sex STRING, age INT, injury STRING, fatal STRING, time STRING, species STRING, investigator_or_source STRING, pdf STRING, href_formula STRING, href_ STRING, case_number1 STRING, case_number STRING, original_order INT)")
                        hiveCtx.sql("INSERT INTO shark1 SELECT * FROM temp_data")
                        val sharkTable = hiveCtx.sql("SELECT * FROM shark1 limit 10")
                        sharkTable.show()
                        //hiveCtx.sql("CREATE TABLE IF NOT EXISTS shark1Partitioned (caseNumber STRING, date STRING, year INT, type STRING, country STRING, area STRING, location STRING, activity STRING, name STRING, sex STRING, age INT, injury STRING, fatal STRING, time STRING, investigator_or_source STRING, pdf STRING, href_formula STRING, href_ STRING, case_number1 STRING, case_number STRING, original_order INT) PARTITIONED BY  (species STRING) row format delimited fields terminated by '\u003B'")
                        //iveCtx.sql("INSERT INTO shark1Partitioned SELECT caseNumber STRING, date STRING, year INT, type STRING, country STRING, area STRING, location STRING, activity STRING, name STRING, sex STRING, age INT, injury STRING, fatal STRING, time STRING, investigator_or_source STRING, pdf STRING, href_formula STRING, href_ STRING, case_number1 STRING, case_number STRING, original_order INT, species STRING FROM shark1")
                        //val part = hiveCtx.sql("SELECT * FROM shark1Partitioned limit 10")
                        //part.show()
                        
                    }



                    // Query for total number of shark attacks since certain date
                    def totalSharkAttacks(){
                        println("Total number of attacks recorded...")
                        val result = hiveCtx.sql("SELECT COUNT(year) AS TotalAttacks FROM shark1 WHERE year > 1950")
                        result.show()
                        result.write.csv("results/totalSharkAttacks")
                        log.write("Executing 'SELECT COUNT(year) FROM shark1 WHERE year > 1950';\n")
                    }
                    
                    // Query for what shark is responsible for the most attacks
                    def sharkResponsible(){
                        println("What Shark is responsible for the most attacks...")
                        val result = hiveCtx.sql("SELECT (species) FROM shark1 WHERE species IS NOT NULL GROUP BY species ORDER BY COUNT(*) DESC")
                        result.show()
                        result.write.csv("results/sharkResponsible")
                        log.write("Executing 'SELECT (species) FROM shark1 GROUP BY species ORDER BY COUNT(*) DESC';\n")
                    }

                    // Query for location of most shark attacks
                    def locationMostSharkAttacks(){
                        println("Location of Most Shark Attack...")
                        val result = hiveCtx.sql("SELECT (country) FROM shark1 WHERE country IS NOT NULL GROUP BY country ORDER BY COUNT(*) DESC")
                        result.show()
                        result.write.csv("results/locationMostSharkAttacks")
                        log.write("Executing 'SELECT (country) FROM shark1 WHERE country IS NOT NULL GROUP BY country ORDER BY COUNT(*) DESC';\n")
                    }

                     
                    // STILL NEED TO FIGURE OUT THIS QUERY
                    // Query for what time of day do most shark attacks occur
                    def timeOfDaySharkAttack(){
                        println("Time most shark attacks occur...")
                        //val result = hiveCtx.sql("SELECT COUNT(year) FROM shark1 WHERE year > 999") // STILL NEED TO FIGURE OUT THIS QUERY
                        //val result1 = hiveCtx.sql("CREATE VIEW AvgTimeOfAttacked AS SELECT time, CONVERT(SUBSTRING(time,1,2), UNSIGNED INTEGER) AS timeConvert FROM shark1")
                        val result1 = hiveCtx.sql("CREATE VIEW IF NOT EXISTS AvgTimeOfAttacked AS SELECT CAST(regexp_replace(time, 'h00', '') AS int) AS time FROM shark1 WHERE time IS NOT NULL LIMIT 10")
                        val result2 = hiveCtx.sql("SELECT AVG(time) FROM AvgTimeOfAttacked WHERE time IS NOT NULL")
                        //result.show()
                        //result.write.csv("results/timeOfDaySharkAttack")
                        result2.show()
                        result2.write.csv("results/timeOfDaySharkAttack")
                        //?log.write("Executing 'NEED TO ADD LOG HERE');\n")
                    }
                    // cast(str_column as int)


                    // Query for the number of provoked and unprovoked shark attacks
                    def provokedUnprovokedAttacks(){
                        println("Number of Provoked and Unprovoked Attacks")
                        val result = hiveCtx.sql("SELECT type, Count(type) AS whichTypeMost FROM shark1 WHERE type IS NOT NULL GROUP BY type ORDER BY whichTypeMost DESC")
                        result.show()
                        result.write.csv("results/provokedUnprovokedAttacks")
                        log.write("Executing 'SELECT typeAttack, Count(typeAttack) AS whichTypeMost FROM shark1 GROUP BY typeAttack ORDER BY whichTypeMost';\n")
                    }

                    // Query for average age range of people attacked
                    def ageRangePeopleAttacked(){
                        println("Average Age of Both Males & Female Attacked")
                        val result = hiveCtx.sql("SELECT sex, AVG(age) AS averageAgeAttacked FROM shark1 WHERE age IS NOT NULL AND sex IS NOT NULL GROUP BY sex")
                        result.show()
                        result.write.csv("results/avgAgePeopleAttacked")
                        //result.write.mode(SaveMode.Overwrite).csv("results/avgAgePeopleAttacked")
                        log.write("Executing 'SELECT sex, AVG(age) AS averageAgeAttacked FROM shark1 GROUP BY sex';\n")
                    }


                            
                }

                //? to overwrite csv file if already exist result.write.mode(SaveMode.Overwrite).csv("filename")
                //? take first 2 digits of time as int then use that to find time of day

            

            
                            
            }catch {
                     case e: Exception => e.printStackTrace            

            }
                connection.close()


            log.close()


    }
  
}
