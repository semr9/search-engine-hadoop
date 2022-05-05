const AWS = require('aws-sdk');
const fs = require('fs');
const cors = require('cors');
const express = require("express");

const filesNamesID = {  "5722018235": "1",
                        "5722018301": "2",
                        "5722018101": "3",
                        "5722018496": "4",
                        "5722018508": "5" };
const filesNamesIDList = [
    "5722018235",
    "5722018301",
    "5722018101",
    "5722018496",
    "5722018508" 
]  

//************** */ FULL DATA ***********************
function calculateIdsDocFull(){
    let start_doc_id_base = "5722018";
    let start_doc_id_num = 435
    filesNamesIDList = []
    filesNamesID = {}
    for(let i=0; i< 73; i++){
        filesNamesIDList.push(start_doc_id_base + start_doc_id_num.toString());
        filesNamesID[start_doc_id_base + start_doc_id_num.toString()] = (i+1).toString();
        start_doc_id_num++;
    }
}


//settings data
const clusterID = "j-1L1341P85Q0A7"

const region = "us-east-1"
const bucketName = "search-engine-hadoop"
const accessKeyId = "AKIASYQUAQKXJJJVP7KK"
const secretAccessKey = "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"
const PORT = process.env.PORT || 3001;
const dictWords = {};
const wordsToSearch = new Set();
const accessKeyId_EMR ="AKIASYQUAQKXN7VJLQBS"
const secretAccessKey_EMR = "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"
//Initialize
const app = express();
const filesII =[ 'output-1/part-r-00000', 'output-1/part-r-00001' ,'output-1/part-r-00002']
const countSearch = 0;

app.use(
    cors({
        origin: 'http://localhost:3000',
        credentials: true 
    }));

app.use(express.json());
// app.use(express.urlencoded({ extended: false }));

const s3 = new AWS.S3({
    accessKeyId: accessKeyId,
    secretAccessKey: secretAccessKey,
})

let sortSearchList = [];

let  emr = new AWS.EMR({
    accessKeyId: accessKeyId_EMR,
    secretAccessKey: secretAccessKey_EMR,
    region: "us-east-1"
  });

function delay(time) {
    return new Promise(resolve => setTimeout(resolve, time));
} 

  
const addJobToEMR = async () => {
    let params = {
        JobFlowId: clusterID, 
        Steps: [ 
          {
            HadoopJarStep: { 
              Jar: 's3://search-engine-hadoop/javaFile/PageRank.jar',
              Args: [ 
                'PageRank', 
                's3://search-engine-hadoop/pageRank.txt',
                's3://search-engine-hadoop/output-pageRank',
              ],
            //   Args: [ 
            //     'PageRank', 
            //     's3://search-engine-hadoop/pageRank2.txt',
            //     's3://search-engine-hadoop/output-pageRank-2',
            //   ],
            //   MainClass: 'PageRank',
            },
            Name: 'Page-Rank', 
            ActionOnFailure: "CONTINUE"
          },
        ]
      };
    let idJOB = "";
    await emr.addJobFlowSteps(params, function(err, data) {
        if (err) console.log("ADD - JOB:: ", err, err.stack); 
        else{
            console.log("ADD + JOB:: ", data);
            idJOB = data.StepIds[0];
            return idJOB;
        }     
        }).promise();
    return idJOB;
} 

const waitUntilJobFinish = async (resaddJobToEMR)=>{
    let stateComplete = await describeSteps(resaddJobToEMR);
    while( stateComplete != 'COMPLETED'){
        console.log("state ?:: ", stateComplete);
        await delay(30000);
        stateComplete = await describeSteps(resaddJobToEMR);   
    }
    console.log("GOOD JOB")
    return ;
}

const describeSteps = async (stepId) => {
    var params = {
        ClusterId: clusterID,
        StepId: stepId
      };
      let stateComplete = "";
      await emr.describeStep(params,  function(err, data) {
        if (err){
             console.log(err, err.stack); // an error occurred
        }else{
            // console.log("data:: ", data);
            stateComplete = data.Step.Status.State;
            // consdata.Status.Stateole.log("DESCRIBE STEPS::", data);   
        }    
    }).promise();
    return stateComplete;
}

const getOrder = (data) =>{
    let list_sentences = data.split(new RegExp("\\n"));
    let values = [];
    for (i =0; i < list_sentences.length; i++) {
        if(list_sentences[i] != '' && list_sentences[i] != null ){
            let b = list_sentences[i].split(new RegExp("\\s+"))
            values.push({ key: filesNamesIDList[parseInt(b[0])-1], value: parseFloat(b[1]) } );
        }
    }
    values.sort((a, b) => (a.value < b.value) ? false:true);
    console.log("values::", values);   
    sortSearchList = [];

    for(let j =0; j < values.length; j++){
        if(wordsToSearch.has(filesNamesID[values[j].key]) ){
            sortSearchList.push(values[j].key);
        }
    }
    console.log("values::", sortSearchList);   
    console.log("wordsToSearch::", wordsToSearch );   
    
    return sortSearchList;
}

const transformDataToDictionaty =  (data) => {
    let list_sentences = data.split(new RegExp("\\n"));
    for (i =0; i < list_sentences.length; i++) {
        let b = list_sentences[i].split(new RegExp("\\s+"))
        dictWords[b[0]] = []
        for (j = 1; j < b.length; j++){
            if(b[j] !== ''){
                dictWords[b[0]].push(b[j])
            }
        }
    }
    return true;
}

const getFilesfromBucket = async (file) =>{
    const params = {
        Bucket: bucketName,
        Key: file,
    };
    
    console.log(file);

    try{
        await s3.getObject(params, function(err, data) {
            // if (err) console.log(err, err.stack); 
            // else 
            // console.log("output-1/part-r-0000"+ file.toString(), "   DONE");
            return transformDataToDictionaty(data.Body.toString());           
        }).promise();
    }catch(err){
        console.log("ERROR_getFilesfromBucket :: ",err);
    }
    return true;
}

const getFilesfromBucketv2 = async (file) =>{
    const params = {
        Bucket: bucketName,
        Key: file,
    };
    
    console.log(file);
    let _data = "";
    try{
        await s3.getObject(params, function(err, data) {
            // if (err) console.log(err, err.stack); 
            // else 
            // console.log("output-1/part-r-0000"+ file.toString(), "   DONE");
            _data = data.Body.toString();           
        }).promise();
    }catch(err){
        console.log("ERROR_getFilesfromBucket :: ",err);
    }
    return _data;
}

const uploadFile = async (key) => {
    let data = fs.readFileSync("./server/pageRank.txt", 'utf8');
    const params = {
        Bucket: bucketName,
        Key: key,
        Body: data
    };

    console.log("uno::",data);
    
    await s3.upload(params, function(s3Err, data) {
        if (s3Err) throw s3Err
        console.log(`File uploaded successfully at ${data.Location}`)
    }).promise();

};

const deleteObject = async(_key) => {
    const params = {
        Bucket: bucketName,
        Key: _key,
    };

    await s3.deleteObject(params, function(err, data) {
        if (err) console.log("delete  error:: ", err, err.stack);
        else     console.log("delete  ok:: ", data);           
    }).promise();

}

const deleteBucket = async () =>{
    var params = {
        Bucket: bucketName, 
        Delete: {
         Objects: [
            {
           Key: 'output-pageRank/part-r-00000', 
          }, 
            {
           Key: 'output-pageRank/_SUCCESS', 
          }
         ], 
         Quiet: false
        }
       };
       await s3.deleteObjects(params, function(err, data) {
         if (err) console.log(err, err.stack); // an error occurred
         else     console.log("DELETE FOLDER:: ", data);           // successful response
       }).promise();
}

const processSearch = async () => {
    //selec the correct ones from total pR
    let newTexx = ""  
    let dataText = fs.readFileSync("./server/pageRankTotal.txt",'utf8') 
    
    //separar y verificar
    // dataTextSplit = dataText.split(new RegExp("\\n"));
    // for (let i = 0; i < dataTextSplit.length; i++){
    //     if (wordsToSearch.has(dataTextSplit[i].split(",")[0])){
    //         if(i != dataTextSplit.length-1){
    //             newTexx = newTexx + dataTextSplit[i] + "\n";
    //         }else{
    //             newTexx = newTexx + dataTextSplit[i];
    //         }
    //     }
    // }
    
    fs.unlinkSync('./server/pageRank.txt');
    fs.writeFileSync( './server/pageRank.txt', dataText )
    // fs.writeFileSync( './server/pageRank.txt', newTexx )

    await deleteObject('pageRank.txt');
    await uploadFile('pageRank.txt');
    console.log("hola1");
    let resaddJobToEMR = await addJobToEMR();
    // console.log("resaddJobToEMR:: ", resaddJobToEMR);
    await waitUntilJobFinish(resaddJobToEMR);
    // READ THE PR RESULTS
    let dataFromPR = await getFilesfromBucketv2("output-pageRank/part-r-00000");
    let orden = getOrder(dataFromPR);
    // DELETE THE BUCKET
    await deleteBucket();
    console.log("******FIN********");
    return ;
}

app.get("/api", (req, res) => {
    // uploadFile();
    // getFilkesfromBucket();
    res.json({ message: "Hello from server!" });
});
  
app.post('/search-data', async function(req, res) {
    const words = req.body.words;
    //separate words and serach in dictionaty
    let listWords = words.split(/[, ']+/);
    // console.log(listWords);
    wordsToSearch.clear();
    let documentID = "";
    for (let i = 0; i < listWords.length; i++){
        if(dictWords[listWords[i]] != undefined){
            for(let j = 0; j < dictWords[listWords[i]].length; j++){
                if(dictWords[listWords[i]][j] != ""){
                    // console.log(" --- ", dictWords[listWords[i]][j]);
                    documentID =   filesNamesID[dictWords[listWords[i]][j].split(":")[0]];
                    wordsToSearch.add(documentID)  
                }
            }
        }
    }
    // console.log(wordsToSearch); 
    // console.log(listWords);
    await processSearch(res);
    res.send( JSON.stringify( sortSearchList ));
});

app.listen(PORT, async  () => {
  console.log(`Server listening on ${PORT}`);
  for( let i=0; i<3; i++ ){
      await getFilesfromBucket(filesII[i]);
  }
});


