# Kelp Coding Challenge

This small project reads data from a CSV file, converts each record into a JSON object, 
and saves the data into a PostgreSQL table.  
After inserting all rows, it prints a simple summary showing the percentage of users in each age group.



## How I ran the project

1. I created a PostgreSQL database called `kelpdb`.
2. Inside it, I made a table named `users` using the given SQL file.
3. I placed my `.env` file with my database connection details.
4. Then I started the server with:node server.js
5. Finally, I triggered the process by sending a POST request to:http://localhost:3000/process


The console printed the “Age-Group % Distribution” after processing my CSV file successfully.


**Author:** Merina Thoppil  


