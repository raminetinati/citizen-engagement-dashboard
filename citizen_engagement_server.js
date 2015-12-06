//Author: Ramine Tinati
//Purpose: Node server for the Citizen Engagement Dashboard

var app = require('http').createServer(handler);
var io = require('socket.io')(app);
var fs = require('fs');
//var config = require('./config');
var dateFormat = require('dateformat');
var mongoose = require('mongoose');


//-------------------
//This is the working port
//PLEASE NOTE: You must configure this in order for this to correctly run. 
//Please find the port associate with your group and configure
app.listen(3005);


//-----------------------------
//SECTION: Databases and models
//We need to connect ot each of the datasets that we're going to be using.
//if you have more, follow the same pattern to connect
//NOTE: If you are running this locally, then the address for these datasets needs to be:
//  mongodb://sotonwo.cloudapp.net/

//POLLUTION DATA
var pm_con = mongoose.createConnection('mongodb://woUser:webobservatory@localhost/pm_25');
var db_pm = pm_con;
db_pm.on('error', console.error.bind(console, 'connection error:'));
db_pm.once('open', function (callback) {
    console.log("connected to database pm_25");
});

//SHENZHEN TWITTER DATA
var shenzhen_con = mongoose.createConnection('mongodb://woUser:webobservatory@localhost/twitter_shenzhen');
var db_shenznen = shenzhen_con;
db_shenznen.on('error', console.error.bind(console, 'connection error:'));
db_shenznen.once('open', function (callback) {
    console.log("connected to database twitter_shenzhen");
});

//GENERAL AIRPOLLUTION DATA
var air_pollution_con = mongoose.createConnection('mongodb://woUser:webobservatory@localhost/air_pollution');
var db_air_pollution = air_pollution_con;
db_air_pollution.on('error', console.error.bind(console, 'connection error:'));
db_air_pollution.once('open', function (callback) {
    console.log("connected to database air_pollution");
});


//INFO: Mongoose requires that for a database connection, we create a schema, 
//INFO: this is then attached to a collection. 
//INFO: Three Schemas (but could use one if you wanted)
var pmDoc = new mongoose.Schema({
  source: String,
  status: String,
});


var twitterDoc = new mongoose.Schema({
  source: String,
  status: String,
});

var twitterDoc_pol = new mongoose.Schema({
  source: String,
  status: String,
});


//INFO: These are the collections and models linked together
var pm_Model = pm_con.model('shenzhen', pmDoc); 
var air_pollution_Model = air_pollution_con.model('tweets_20151100', twitterDoc_pol); 
var shenzhen_Model = shenzhen_con.model('tweets_20151100', twitterDoc); 


//----------------------------
//SECTION: SOCKET IO work

//INFO: When a connection is established with a client, the 'connection' port recieves a handshake
io.on('connection', function (socket) {

     //we want to automatically load the data to the client
     socket.on('load_data', function (data) {
        console.log("Loading Map Data");
        //console.log("emitting filter:", filter); 
        loadShenzhenTweets(socket);  
    });

     //we will then proceed to load the pollution data
     socket.on('load_pollution_data', function (data) {
        console.log("Socket load_pollution_data called");
        //console.log("emitting filter:", filter); 
        loadPollutionTweets(socket);      
    });
});


//----------------------------
//SECTION: Functions 

//INFO: This function retrieves ALL the pollution data in the collection and streams it to the client
function loadPollutionTweets(socket){
    console.log("Loading Pollution Data");

    var toSend = [];

    var stream = air_pollution_Model.find().stream();

    stream.on('data', function (doc) {
        //console.log(doc)
        toSend.push(doc);
        socket.emit("pollution_data", toSend);
        toSend = [];
      // do something with the mongoose document
    }).on('error', function (err) {
      // handle the error
    }).on('close', function () {        
      // the stream is closed
        socket.emit("finished_sending_pollution_data", "");
        loadPM25Data(socket)
    });

}



//########################
//NOTE: General patterns for retrieving data from the db and sending data to the client
// 1. Client requests data using a socket pulse
// 2. Server queries database using stream, sends data back to client via socket
// 3. When all data is finished being sent, server notifies client with new socket pulse
// 4. Server the proceeds to the next dataset.
//NOTE: Currently this happens sequentially, but this is not necessary...!
//#######################

//INFO: This function retrieves all the Shenzhen data and streams it to the client.
function loadShenzhenTweets(socket){

    var toSend = [];
    //the find query will only return documents which have a geo tag.
    //this does not ensure that the tweet is within the the shenzhen region though!
    var stream = shenzhen_Model.find({ "geo": { $ne: null }}).stream();
    stream.on('data', function (doc) {
         
        toSend.push(doc);
        //console.log(doc)
        if(toSend.length>10){
            socket.emit("shenzhen_data", toSend);
            toSend = [];
        }
    
      // do something with the mongoose document
    }).on('error', function (err) {
      console.log(err)
    }).on('close', function () {

        //once we have finished sending all the data, then we want to notify the client.
        socket.emit("finished_sending_data", "");
        loadPollutionTweets(socket)
    });
}

//INFO: This function retrieves ALL the pollution data in the collection and streams it to the client
function loadPM25Data(socket){
    console.log("Loading PM25 Data");

    var toSend = [];

    var stream = pm_Model.find().stream();

    stream.on('data', function (doc) {
        //console.log(doc)
        toSend.push(doc);
        socket.emit("pm25_data", toSend);
        toSend = [];
      // do something with the mongoose document
    }).on('error', function (err) {
      // handle the error
    }).on('close', function () {        
      // the stream is closed

      //here we also need to notify the client that the pm25 data has been updated.
      socket.emit("finished_sending_pm_data", "");
      //currently nothing else in the processing chain.
    });

}





//INFO: Misc functions whihc we use for the HTTP server.
function showErr (e) {
    console.error(e, e.stack);
}

function handler (req, res) {
    res.writeHead(200);
    res.end("");
}

