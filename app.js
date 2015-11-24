var app = require('http').createServer(handler);
var io = require('socket.io')(app);
var fs = require('fs');
//var config = require('./config');
var dateFormat = require('dateformat');
var mongoose = require('mongoose');

//set the http
app.listen(3005);


var pm_con = mongoose.createConnection('mongodb://woUser:webobservatory@localhost/pm_25');

var db_pm = pm_con;
db_pm.on('error', console.error.bind(console, 'connection error:'));
db_pm.once('open', function (callback) {
    console.log("connected to database pm_25");
});


var shenzhen_con = mongoose.createConnection('mongodb://woUser:webobservatory@localhost/twitter_shenzhen');

var db_shenznen = shenzhen_con;
db_shenznen.on('error', console.error.bind(console, 'connection error:'));
db_shenznen.once('open', function (callback) {
    console.log("connected to database twitter_shenzhen");
});


var air_pollution_con = mongoose.createConnection('mongodb://woUser:webobservatory@localhost/air_pollution');

var db_air_pollution = air_pollution_con;
db_air_pollution.on('error', console.error.bind(console, 'connection error:'));
db_air_pollution.once('open', function (callback) {
    console.log("connected to database air_pollution");
});


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


var pm_Model = pm_con.model('shenzhen', pmDoc); 

var air_pollution_Model = air_pollution_con.model('tweets_20151100', twitterDoc_pol); 


var shenzhen_Model = shenzhen_con.model('tweets_20151100', twitterDoc); 




io.on('connection', function (socket) {

     socket.on('load_data', function (data) {
        console.log("Loading Map Data");
        //console.log("emitting filter:", filter); 
        loadShenzhenTweets(socket);  
    });

     socket.on('load_pollution_data', function (data) {
        console.log("Socket load_pollution_data called");
        //console.log("emitting filter:", filter); 
        loadPollutionTweets(socket);      
    });
});


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
    });



    // var response = [];
    // air_pollution_Model.find(function (err, responses) {
    // if (err) return console.error(err);
    //  console.log(responses);
    //  try{
    //     socket.emit("pollution_data", responses.slice((responses.length-20), (responses.length-1)));
    // }catch(e){
    //     socket.emit("pollution_data", responses);
    //     }
    // })
}



function loadShenzhenTweets(socket){


    var toSend = [];
    var stream = shenzhen_Model.find({ "geo": { $ne: null }}).stream();

    stream.on('data', function (doc) {
         
    // if(doc.geo != undefined){
        toSend.push(doc);
        //console.log(doc)
        if(toSend.length>10){
            socket.emit("shenzhen_data", toSend);
            toSend = [];
        }
    // }
        // socket.emit("shenzhen_data", doc);

      // do something with the mongoose document
    }).on('error', function (err) {
      console.log(err)
    }).on('close', function () {
        socket.emit("finished_sending_data", "");
        loadPollutionTweets(socket)
    });









    // var response = [];
    // shenzhen_Model.find(function (err, responses) {
    // if (err) return console.error(err);
    //  //console.log(responses);
    //  try{
    //     socket.emit("shenzhen_data", responses.slice((responses.length-10), (responses.length-1)));
    // }catch(e){
    //  	socket.emit("shenzhen_data", responses);
    //     }
    // })
}


function showErr (e) {
    console.error(e, e.stack);
}

function handler (req, res) {
    res.writeHead(200);
    res.end("");
}
