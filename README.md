# REST API to process a dataset of trips

This project was built as part of a challenge to process a dataset containing data about trips.

Python was used as the main language for this project. So, in order to run it, you will need Python 3.10+ installed in your machine.


## Setup

1. Create a folder where you want to store the files that will be cloned from github.
2. Inside the folder, clone the github repository from https://github.com/erdinand/apitrips.git.
3. Create a Python virtual environment to install the packages without affecting your main Python installation.
4. Activate the virtual environment that was created.
5. You will notice that prompt will change, showing that now you're in the virtual environment.
6. Install the required packages for the project according to requirements.txt file.
7. Execute the Python script responsible for starting the REST API service.

Following you can see an example of the installation in a MacOS system. The steps for Linux is very similar and for Windows some commands will be different, but in general the steps mentioned previously and the commands above will give you a good guideline on how configure and run the project.

```bash
mkdir ~/myfolder

cd ~/myfolder

git clone https://github.com/erdinand/apitrips.git

cd apitrips

python3 -m venv .venvprojapi

source .venvprojapi/bin/activate

python3 -m pip install -r requirements.txt

python3 api.py
```


## Usage

When you execute the Python script above, a message like "Running on http://127.0.0.1:5000" will appear. This address is the address of the REST API that you will use to make requests.

You can use tools like Postman, Curl or a create a custom script able to send GET/POST requests to REST APIs. In my case, due to time restrictions, I chose the free version of Postman. Above I explain how every request to each endpoint works and how they can be executed using Postman.

1. **/load**
     - Call this endpoint to start the loading process. You can use GET or POST to make the request and it's not necessary to send any parameters. 
     - This resource will load the CSV file trips.csv, located on the *source* project's folder, make transformations in some columns, apply some clustering techniques to find similar routes and, finally, load the transformed data in a local SQL database using SQLite, which will generate a file called 'trips-db.db' into the *database* project's folder.
     - The above process is asyncronous. Once the REST API receives the request, it will start a thread to do all the steps to load the CSV data, returning a message that the loading process has been started and letting the user know that there's another endpoint where the user can receive updates about the loading status.
     - ![Example using Postman to call /load](images/postman-01-load.png?raw=true "Example using Postman to call /load")

2. **/load-status**
     - This is a web socket that establishes communication with the client, allowing the server to send messages to the client. This way, the client does not need to make polling requests to have updates about the status of the loading process.
     - Notice that for this specific endpoint, you don't need to use HTTP in the URL to establish a connection. In Postman, inside "My Workspace", you can create a new "WebSocket Request", enter the address of the server with /load-status in the end and then click on "Connect". It's not necessary to send any parameters. Once connected, the messages sent by the API will appear in the "Messages" window.
     - ![Example using Postman to call /load-status](images/postman-02-load-status.png?raw=true "Example using Postman to call /load-status")

3. **/weekly-trips-average/by-region**
     - Call this endpoint to get the weekly average number of trips of a region. You can only use POST to make the request and it's necessary to send a JSON containing the name of the region you want to see the result. Example:
        -   {"region": "Turin"}
     - API will consider all routes where region was equal the received parameter and then calculate the quantity of trips by week and the average number.
     - Once the result is calculated, the API will return a JSON with the result. Example:
        -   {"result": 9.3}
     - ![Example using Postman to call /weekly-trips-average/by-region](images/postman-03-avg-by-region.png?raw=true "Example using Postman to call /weekly-trips-average/by-region")

4. **/weekly-trips-average/by-bounding-box**
     - Call this endpoint to get the weekly average number of trips according a bounding box. You can only use POST to make the request and it's necessary to send a JSON coordinates of the bounding box that you want to consider. Example (coordinates can be separated by comma or space):
        -   {
                "north-east": "45.1212, 7.5193",
                "south-east": "45.0390, 7.5193",
                "north-west": "45.1212, 7.8129",
                "south-west": "45.0390, 7.8129"
            }
     - API will consider all routes where origin and destination latitude/longitude are between the bounding box coordinates received as parameter. Then, it will calculate the quantity of trips by week and the average number.
     - Once the result is calculated, the API will return a JSON with the result. Example:
        - {"result": 4.8}
     - ![Example using Postman to call /weekly-trips-average/by-bounding-box](images/postman-04-avg-by-bounding-box.png?raw=true "Example using Postman to call /weekly-trips-average/by-bounding-box")


## Observations 

 - questions.sql contains the queries that provide the answers to the questions proposed by the challenge.
