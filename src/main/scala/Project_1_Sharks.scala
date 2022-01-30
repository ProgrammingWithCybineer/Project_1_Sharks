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
        val password = "########" // Update to include your password
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
                    

                    
                    //def mainMenu(){
                    println("###################################")
                    println("Please choose from the menu below: ")
                    println("(1) Create Account ")
                    println("(2) User Log In ")
                    println("(3) Admin Log In ")
                    println("(0) Quit Program")
                    println("###################################")
                    var choice= (scanner.nextInt())
                    scanner.nextLine()
                    //}  

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
                        while ( resultSet.next() ) {
                            if (resultSet.getString(1) == "1") {
                                println("You Have Logged In Successful")
                                userMenu()
                            }else{
                                println("Username/password combo not found. Try again!")
                                userLogIn()
                                                    
                            }
                        }
                        
                                                    
                    }    
                          

                    def userMenu(){
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

                            
                        }else if (choice2 == 2) {
                            println(" What shark is responsible for the most shark attacks?")
                            sharkResponsible()
                            
                        }else if (choice2 == 3) {
                            println(" Where do the most shark attacks happen?")
                            locationMostSharkAttacks()
                            
                        }else if (choice2 == 4) {
                            println(" What time of day do most shark attacks happen?")
                            timeOfDaySharkAttack()
                            
                        }else if (choice2 == 5) {
                            println(" Are most shark attacks provoked or unprovoked?")
                            provokedUnprovokedAttacks()
                            
                        }else if (choice2 == 6) {
                            println(" What is the age range of people attacked by sharks?")
                            ageRangePeopleAttacked()

                        }else if (choice2 == 0) {
                            exitProgram()
                            
                        }else if (( choice != 0 || choice != 1 || choice != 2 || choice != 3 || choice != 4 || choice != 5|| choice != 6)) {
                            println(" Not a valid choice, please try again!!!")
                            userMenu()
                            
                        }
                    }

                    
                    
                    // logging in as Admin
                    def adminLogIn(){
                        val statement2 = connection.createStatement()
                        println("Type Admin UserName")
                        userName = scanner.nextLine().trim()
                        println("")
                        println("Type Admin Password")
                        userPassword = scanner.nextLine().trim()
                        println("")     
                        val resultSet2 = statement2.executeQuery("SELECT COUNT(*) FROM adminAccount WHERE userAdmin='"+userAdmin+"' AND adminPassword='"+adminPassword+"';")
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
                        println(" This will update the data ")
                        
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


                    // Delete User from data base
                    def deleteUser(){
                        println("")
                        println(" this will delete the selected user")
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
                        println("                                       DO STAY SAFE IN OUR OCEANS                                        ")
                        println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
                        println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
                        println("")        
                        println("")
                        println("")
                        shark = false
                        
                    }

                    def sharkAttackData(){
                        val output = hiveCtx.read
                            .format("csv")
                            .option("inferSchema", "true")
                            .option("header", "true")
                            .load("input/global-shark-attack.csv")
                        output.limit(25).show() // will print out the first 25 lines


                        // This code will create a temp view of the dataset you used and load the data into a permanent table
                        // inside of Hadoop. this will persist the data and only require this code to run once.
                        // After initialization this code will and creation of output will not me necessary
                        output.createOrReplaceTempView("temp_data")
                        hiveCtx.sql("CREATE TABLE IF NOT EXISTS shark1 (caseNumber STRING, date STRING, year INT, type STRING, country STRING, area STRING, location STRING, activity STRING, name STRING, sex STRING, age INT, injury STRING, fatal STRING, time STRING, species STRING, investigator_or_source STRING, pdf STRING, href_formula STRING, href_STRING, case_number1 STRING, case_number STRING, original_order INT)")
                        hiveCtx.sql("INSERT INTO shark1 SELECT * FROM temp_data")


                        //?Copy line 400 and put here for partitioning
                        //?INSERT INTO new_table SELECT cast(old_string_col AS DATE) FROM old_table got column that contain date

                    }




                    def totalSharkAttacks(){
                        println("Total number of attacks recorded")
                        val result = hiveCtx.sql("SELECT COUNT(year) FROM shark1 WHERE year > 999;")
                        result.show()
                        result.write.csv("results/totalSharkAttacks")
                    }
                    
                    def sharkResponsible(){
                        println("most attacks")
                        val result = hiveCtx.sql("SELECT (species) FROM shark1 GROUP BY species ORDER BY COUNT(*) DESC;")
                        result.show()
                        result.write.csv("results/totalSharkAttacks")
                    }

                    def locationMostSharkAttacks(){
                        println("most attacks")
                        val result = hiveCtx.sql("SELECT (area) FROM shark1 GROUP BY area ORDER BY COUNT(*) DESC;")
                        result.show()
                        result.write.csv("results/totalSharkAttacks")
                    }

                    def timeOfDaySharkAttack(){
                        println("most attacks")
                        val result = hiveCtx.sql("SELECT COUNT(year) FROM shark1 WHERE year > 999;")
                        result.show()
                        result.write.csv("results/totalSharkAttacks")
                    }

                    def provokedUnprovokedAttacks(){
                        println("most attacks")
                        val result = hiveCtx.sql("SELECT COUNT(year) FROM shark1 WHERE year > 999;")
                        result.show()
                        result.write.csv("results/totalSharkAttacks")
                    }

                    def ageRangePeopleAttacked(){
                        println("most attacks")
                        val result = hiveCtx.sql("SELECT COUNT(year) FROM shark1 WHERE year > 999;")
                        result.show()
                        result.write.csv("results/totalSharkAttacks")
                    }


                            
                }
            

            
            val resultSet1 = statement.executeQuery("SELECT * FROM Sharks")
            log.write("Executing 'SELECT * FROM Sharks' ;\n")
            while ( resultSet1.next() ) {
                print(resultSet1.getString(1) + " " + resultSet1.getString(2) + " " + resultSet1.getString(3) + " " + resultSet1.getString(4) + " " + resultSet1.getString(5) + " " + resultSet1.getString(6) + " " + resultSet1.getString(7))
                println("")
                println("")
                
            }

                
            }catch {
                     case e: Exception => e.printStackTrace            

            }
                connection.close()


            log.close()







    }
  




}
