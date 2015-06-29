var allEvents;
var stageCompleted;
var taskCompleted;
var taskStartEnd;
//map real Stage ID to index in array (0, 1, 2...)
//Example : stageIndexMapping[50] = 1 if Stage 50 is the second in execution order
var stageIndexMapping = {};

var parsedStages;
var parsedTasks;


function parseLog(text) {

    allEvents = text.split("\n").map(function(x) {
        if (x) {
            js = JSON.parse(x);
            if (js != undefined) {
                return js
            } else {
                console.log("Error parsing json " + x)
            }
        }
    });
    var cs = getCompletedStages();
    var ct = getCompletedTasks();

    linkCompletedTasksToStages();
    var sw = getShuffleWrite();
    var tSE = getTasksStartEnd()
    loadStageGraph('#container', [cs, ct], 'Stages/Tasks Duration', 'Stage', 'Time (ms)', formatter)
    loadStageGraph('#shuffleWrite', [sw], 'ShuffleWrite Size', 'Stage', 'Byte', formatter)
    loadStageGraph('#tasksTime', [tSE],' Task startTime endTime ',  'Task ID', 'Time (ms)',formatter, false)

};

function getCompletedStages() {
    stageCompleted = filterEvent(allEvents, "SparkListenerStageCompleted");
    parsedStages = stageCompleted.map(function(x, index) {
        var info = x["Stage Info"]
        stageIndexMapping[info["Stage ID"]] = index;
        return {
            x: index,
            y: info["Completion Time"] - info["Submission Time"],
            stage : {
            duration : info["Completion Time"] - info["Submission Time"],
            stageID: info["Stage ID"],
            numTasks: info["Number of Tasks"],
            taskList: []
            }
        }
    });
    return {
        type: "line",
        ctype: "stage",
        id: 0,
        color: 'rgba(223, 83, 83, .5)',
        data: parsedStages
    }
}

function getCompletedTasks() {
    taskCompleted = filterEvent(allEvents, "SparkListenerTaskEnd");
    parsedTasks = taskCompleted.map(function(x, index) {
        var info = x["Task Info"]
        var metrics = x["Task Metrics"]
            //can be undefined for some tasks
        var input = metrics["Input Metrics"]
        var shuffleWrite = metrics["Shuffle Write Metrics"]
        var shuffleRead = metrics["Shuffle Read Metrics"]
            // console.log(info)
            //  console.log(info);
            // return [info["Stage ID"],  info["Completion Time"]- info["Submission Time"]]
        return {
            ctype: "task",
            x: stageIndexMapping[x["Stage ID"]],
            y: info["Finish Time"] - info["Launch Time"],
            task: {
                realID: info["Task ID"],
                stageID: x["Stage ID"],
                host: info["Host"],
                launchTime : info["Launch Time"],
                finishTime : info["Finish Time"],
                duration : info["Finish Time"] - info["Launch Time"],
                deserializeTime: metrics["Executor Deserialize Time"],
                resultSize: metrics["Result Size"],
                runTime: metrics["Executor Run Time"],
                inputFrom: input == undefined ? undefined : input["Data Read Method"],
                inputSize: input == undefined ? undefined : input["Bytes Read"],
                shuffleWritten: shuffleWrite == undefined ? undefined : shuffleWrite["Shuffle Bytes Written"],
                shuffleWriteTime: shuffleWrite == undefined ? undefined : shuffleWrite["Shuffle Write Time"],
                shuffleReadRemoteBlocks: shuffleRead == undefined ? undefined : shuffleRead["Remote Blocks Fetched"],
                shuffleReadLocalBlocks: shuffleRead == undefined ? undefined : shuffleRead["Local Blocks Fetched"],
                shuffleReadFetchWait: shuffleRead == undefined ? undefined : shuffleRead["Fetch Wait Time"],
                shuffleReadRemoteBytes: shuffleRead == undefined ? undefined : shuffleRead["Remote Bytes Read"],
            }
        }
    });
    return {
        type: 'scatter',
        name: "Tasks",
        id: 1,
        color: 'rgba(68, 170, 213, 0.8)',
        data: parsedTasks
    }
}


function getShuffleWrite() {
    shuffleWriteTasks = parsedTasks.map(function(elem, index) {
        return {
             ctype: "task",
            x: elem["x"],
            y: elem.task.shuffleWritten,
            task : elem.task
        }
    });
    return {
        type: 'scatter',
        name : 'Shuffle Write',
        id: 2,
        color: 'rgba(68, 170, 213, 0.8)',
        data: shuffleWriteTasks
    }
}

function getTasksStartEnd() {
    taskStartEnd = parsedTasks.map(function(elem, index) {
        return {
            ctype: "task",
            x: elem.task.realID,
            low: elem.task.launchTime,
            high : elem.task.finishTime,
            task : elem.task
        }
    });

  function compare(a,b) {
   return a.x - b.x;
 }
   taskStartEnd.sort(compare);
//now that it is sorted, substract the start time of the first
var min = taskStartEnd[0].low;
taskStartEnd.map(function(elem) {
     elem.low -= min;
     elem.high -= min;
});


    return {
        type: 'columnrange',

        name : 'Task Start End',
        id: 3,
        color: 'rgba(68, 170, 213, 0.8)',
        data: taskStartEnd
        }
}

function linkCompletedTasksToStages() {
    for (var i = 0; i < parsedTasks.length; i++) {
        var tID = parsedTasks[i].task.realID;
        var sID = parsedTasks[i].task.stageID;
        var index = stageIndexMapping[sID];
        parsedStages[index].stage.taskList.push(tID);
    }

}


function filterEvent(array, eventName) {
    return array.filter(function(x) {
        if (x != undefined) {
            return (x["Event"] == eventName);
        } else {
            return false
        }
    });

}

function loadStageGraph(element, data, title, xtitle, ytitle, formatter, inverted) {
    inverted = inverted || false;
    $(function() {
        $(element).highcharts({
            chart: {
inverted : inverted,
                zoomType: 'xy'
            },
            title: {
                text: title
            },
            xAxis: {
                title: {
                    enabled: true,
                    text: xtitle
                },
                startOnTick: true,
                endOnTick: true,
                showLastLabel: true,
                min: 0
            },
            yAxis: {
                title: {
                    text: ytitle
                },
                min: 0
            },
            legend: {
                layout: 'vertical',
                align: 'left',
                verticalAlign: 'top',
                x: 100,
                y: 70,
                floating: true,
                backgroundColor: (Highcharts.theme && Highcharts.theme.legendBackgroundColor) || '#FFFFFF',
                borderWidth: 1
            },
            plotOptions: {
                scatter: {
                    marker: {
                        radius: 5,
                        states: {
                            hover: {
                                enabled: true,
                                lineColor: 'rgb(100,100,100)'
                            }
                        }
                    },
                    states: {
                        hover: {
                            marker: {
                                enabled: false
                            }
                        }
                    }
                },
                series: {
                    cursor: 'pointer',

                    point: {
                        events: {
                            click: function(e) {
                                details(this.options)
                            }
                        }
                    }
                }

            },
            tooltip: {
                formatter: formatter
                // crosshairs : true
                // headerFormat: '<b>{series.name} {point.x}</b>',
                // pointFormat: '<br>{point.y} ms'
            },
            series: data
        })
    })
}

function formatter() {

    if (this.point.ctype == 'stage') {
        return stageFormatter(this);
    }
    if (this.point.ctype == 'task') {
    //if (this.series.options.id ==1 || this.series.options.id ==2 ){
        return taskFormatter(this);
    }

}

function stageFormatter(th) {
    //console.log(this.point.realID)
    // console.log(this.y)
    s = '<b>Stage : ' + th.point.stageID + '</b>';
    s += '<br> Num Tasks : ' + th.point.stage.numTasks;
    s += '<br> Duration : ' + th.point.y + ' ms';
    s += '<br> Tasks  : ' + th.point.stage.taskList;
    return s;
}


function taskFormatter(th) {
    //console.log(this.point.realID)
    // console.log(this.y)
    s = '<b>Task : ' + th.point.task.realID + '</b>';
    s += '<br> Stage ID : ' + th.point.task.stageID;
    s += '<br> Host : ' + th.point.task.host;
    s += '<br> Duration : ' + th.point.y + ' ms';
    s += '<br> Deserialize : ' + th.point.task.deserializeTime + ' ms';
    s += '<br> Run time : ' + th.point.task.runTime + ' ms';
    if (th.point.task.inputFrom != undefined) {
        s += '<br> Input From : ' + th.point.task.inputFrom;
        s += '<br> Input Size : ' + Math.round(th.point.task.inputSize / 1024) + ' KB';
    }
    return s;
}

function details(point) {
    if (point.ctype == 'task') {
        var elem = point.task;
        s = '<b>Task : ' + elem.realID + '</b>';
        s += '<br> Stage ID : ' + elem.stageID;
        s += '<br> Host : ' + elem.host;
        s += '<br> Duration : ' + elem.duration + ' ms';
        s += '<br> Deserialize : ' + elem.deserializeTime + ' ms';
        s += '<br> Run time : ' + elem.runTime + ' ms';
        s += '<br> Result Size : ' + Math.round(elem.resultSize / 1024, 2) + ' KB';
        if (elem.inputFrom != undefined) {
            s += '<details>'
            s += '<summary> Input </Summary>'
            s += 'Input From : ' + elem.inputFrom;
            s += '<br> Input Size : ' + Math.round(elem.inputSize / 1024, 2) + ' KB';
            s += '</details>'
        }
        if (elem.shuffleWritten != undefined) {
            s += '<details>'
            s += '<summary> Shuffle Write </Summary>'
            s += 'Bytes Written : ' + Math.round(elem.shuffleWritten / 1024, 2) + ' KB';
            s += '<br> Shuffle Time : ' + elem.shuffleWriteTime + ' ms';
            s += '</details>'
        }
        if (elem.shuffleReadRemoteBlocks != undefined) {
            s += '<details>'
            s += '<summary> Shuffle Read </Summary>'
            s += 'Remote Blocks : ' + elem.shuffleReadRemoteBlocks;
            s += '<br> Local Blocks : ' + elem.shuffleReadLocalBlocks;
            s += '<br> Fetch Wait Time : ' + elem.shuffleReadFetchWait + ' ms';
            s += '<br> Remote Bytes Read : ' + Math.round(elem.shuffleReadRemoteBytes / 1024, 2) + " ";
            s += '</details>';

        }

    } else {
         var elem = point.stage;
        s = '<b> Stage ID : ' + elem.stageID + '</b>';
        s += '<br> Duration : ' + elem.duration + ' ms';
        s += '<br> Tasks  : ' + elem.taskList;
    }
    $("#details").empty().html(s);
}