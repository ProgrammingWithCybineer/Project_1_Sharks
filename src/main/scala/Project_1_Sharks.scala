import java.util.Scanner
import java.sql.DriverManager
import java.sql.Connection
import com.mysql.cj.xdevapi.UpdateStatement
import java.io.File
import java.io.PrintWriter








object Project_1_Sharks {

    def main(args: Array[String]): Unit = {
         // declaring all variables needed for program
        var scanner = new Scanner(System.in)
        val log = new PrintWriter(new File("Sharks.log"))
        val driver = "com.mysql.jdbc.Driver"
        // Modify for whatever port you are running your DB on
        val url = "jdbc:mysql://localhost:3306/Project_1_Sharks"
        val username = "root"
        //? DON'T FORGET TO DELETE PASSWORD BEFORE PUSHING TO GITHUB
        val password = "ProgramWithNoFears920" // Update to include your password
        var connection:Connection = null 
        var userName = ""
        var userPassword = ""
        var userPassword2 = ""
        var userAdmin = ""
        var adminPassword = ""
        var choice = ""
        var choice2 = ""
        var userMenu = ""
        var shark = true
        
        
        





         
        try{
            
            // make the connection
            Class.forName(driver)
            connection = DriverManager.getConnection(url, username, password)  
            //val statement = connection.createStatement()

            val statement = connection.createStatement()  


            // Welcome screen to the app
            println("                                 *                                                        ")
            println("                                * *                       *                               ")
            println("                               *   *                     * *                              ")
            println("                              *     *                   *   *                             ")
            println("                                  ----------------------    -------------                 ")
            println("                                                                                          ")
            println("                                                                                          ")
            println("                                                                                          ")
            println("")        
            println("")


                  
                // Application loop
                while (shark){
                    //def mainMenu(){
                    println("Please choose from the menu below: ")
                    println("(1) Create Account ")
                    println("(2) User Log In ")
                    println("(3) Admin Log In ")
                    var choice= (scanner.nextInt())
                    scanner.nextLine()
                    //}  

                        if (choice == 1){
                            createAccount()
                                
                        }else if(choice == 2){
                            userLogIn()

                        }else if(choice == 3){
                            adminLogIn()
                                
                        }else if(( choice != 1 || choice != 2 || choice != 3 )){
                            println(" Not a valid choice. Try again")
                             //mainMenu()
                        }
                                
                    //val resultSet4 = statement.executeUpdate("UPDATE Players SET fairy = ('"++"') WHERE playerName = ('"++"') ;")    
                    //log.write("Executing 'UPDATE Players SET fairy = ('"++"') WHERE playerName = ('"++"') ;\n") 
                    // Requesting User Input
                    
                        
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
                            userMenu()

                        }else if (userPassword != userPassword2){
                            println(" Passwords do not match please try again")
                            println("Please type your password")
                            userPassword = (scanner.nextLine())
                            println("")
                            println("Please type your retype password")
                            userPassword2 = (scanner.nextLine())
                            println(" Account has been created")
                            userMenu()
                        }
                        //Updating table with userName and password after creating a new account
                        val resultSet2 = statement.executeUpdate("INSERT INTO Sharks (userName, userPassword, userPassword2) VALUES ('"+userName+"', '"+userPassword+"',  '"+userPassword2+"');")
                        log.write("Executing 'INSERT INTO Sharks (userName, userPassword, userPassword2) VALUES ('"+userName+"', '"+userPassword+"',  '"+userPassword2+"');\n")

                    }    
                    //User logging in
                    def userLogIn(){
                        println(" Please type a User Name")
                        userName = (scanner.nextLine())
                        println("")
                        println(" Please type A Password")
                        userPassword = (scanner.nextLine())
                        if (userPassword == userPassword2){
                            userMenu()
                            
                        }else if (userPassword != userPassword2){
                            println(" Incorrect password. Try again !!!")
                            userLogIn()
                        }
                            
                    }    
                        //val resultSet5 = statement.executeUpdate("UPDATE Players SET babyDragon = ('"+babyDragon+"') WHERE playerName = ('"+playerName+"') ;")
                        //log.write("Executing 'UPDATE Players SET babyDragon = ('"+babyDragon+"') WHERE playerName = ('"+playerName+"') ;\n")  
                    def userMenu(){
                        println(" What type of data would you like to view. Please select below: ")
                        println("")
                        println(" (1) Where do the most shark attacks happen?")
                        println("")
                        println(" (2) What shark is responsible for the most shark attacks?")
                        println("")
                        println(" (3) Do male sharks attack more than female sharks?")
                        println("")
                        println(" (4) What time of day do most shark attacks happen?")
                        println("")
                        println(" (5) Do sharks attacks follow shark migration patterns?")
                        println("")
                        println(" (6) What is the age range of the sharks that cause the most shark attacks?")
                        println("")
                        var choice2 =  (scanner.nextInt())
                        (scanner.nextLine()) 
                        if (choice2 == 1){
                            println(" choice 1")                            
                            
                        }else if (choice2 == 2) {
                            println(" choice 2.")
                            
                        }else if (choice2 == 3) {
                            println(" choice 3.")
                            
                        }else if (choice2 == 4) {
                            println(" choice 4.")
                            
                        }else if (choice2 == 5) {
                            println(" choice 5.")
                            
                        }else if (choice2 == 6) {
                            println(" choice 6.")
                            
                        }else if (( choice != 1 || choice != 2 || choice != 3 || choice != 4 || choice != 5|| choice != 6)) {
                            println(" Not a valid choice, please try again!!!")
                            
                        }
                    }

                    
                    
                    // do not pet fairy
                    def adminLogIn(){
                        println("Type Admin UserName")
                        userName = (scanner.nextLine())
                        println("")
                        println("Type Admin Password")
                        userPassword = (scanner.nextLine())
                        println("")
                        //var query = "SELECT userPassword FROM persons Where userName = " + userName  +"; "
                        if (userPassword == userPassword2){
                            println(" Log In Successful")
                            adminMenu()


                         
                        }           
                    }
                        // do not pet baby dragon
                        def adminMenu(){
                            println("")
                            println(" Welcome Admin. Please choose from below: ")
                            println("")
                            println(" (1) Update dataset ")
                            println("")
                            println(" (2) Delete a User from the Database")
                            println("")
                            println(" (3) Print all Users in database")
                            println("")
                            var choice2 =  (scanner.nextInt())
                            (scanner.nextLine()) 
                            if (choice2 == 1){
                                println(" choice 1")                            
                            
                            }else if (choice2 == 2) {
                                println(" choice 2.")
                            
                            }else if (choice2 == 3) {
                                println(" choice 3.")
                            
                            }else if (( choice != 1 || choice != 2 || choice != 3 || choice != 4 || choice != 5|| choice != 6)) {
                            println(" Not a valid choice, please try again!!!")
                            
                            }
                        }


                            
                }
            

            
            val resultSet1 = statement.executeQuery("SELECT * FROM Players")
            log.write("Executing 'SELECT * FROM Players' ;\n")
            while ( resultSet1.next() ) {
                print(resultSet1.getString(1) + " " + resultSet1.getString(2) + " " + resultSet1.getString(3) + " " + resultSet1.getString(4) + " " + resultSet1.getString(5) + " " + resultSet1.getString(6) + " " + resultSet1.getString(7))
                println("")
                println("")
                
            }

            // Query to delete database entry where play age is less then 5
            println("")
            println(" Next query will delete all players game who's age is less than 4")
            println("")
            // this is for the total number of people who ended the game with health above 2
            val resultSet8 =  statement.executeUpdate("DELETE FROM Players WHERE age < 4 ;")
            log.write("Executing 'DELETE FROM Players WHERE age < 2' \n")
            val resultSet9 = statement.executeQuery("SELECT * FROM Players")
            log.write("Executing  'SELECT * FROM Players' ;\n")
            while ( resultSet9.next() ) {
                 print(resultSet9.getString(1) + " " + resultSet9.getString(2) + " " + resultSet9.getString(3) + " " + resultSet9.getString(4) + " " + resultSet9.getString(5) + " " + resultSet9.getString(6) + " " + resultSet9.getString(7))
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
