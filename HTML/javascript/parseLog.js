var allEvents;
var stageCompleted;
var taskCompleted;
var taskStartEnd;
var boxplotData;
//map real Stage ID to index in array (0, 1, 2...)
//Example : stageIndexMapping[50] = 1 if Stage 50 is the second in execution order
var stageIndexMapping = {};

var parsedStages;
var parsedTasks;
//the time of the first event
var minTime;

function darkTheme() {
    Highcharts.createElement('link', {
        href: '//fonts.googleapis.com/css?family=Unica+One',
        rel: 'stylesheet',
        type: 'text/css'
    }, null, document.getElementsByTagName('head')[0]);

    Highcharts.theme = {
        colors: ["#2b908f", "#90ee7e", "#f45b5b", "#7798BF", "#aaeeee", "#ff0066", "#eeaaee",
            "#55BF3B", "#DF5353", "#7798BF", "#aaeeee"
        ],
        chart: {
            backgroundColor: {
                linearGradient: {
                    x1: 0,
                    y1: 0,
                    x2: 1,
                    y2: 1
                },
                stops: [
                    [0, '#2a2a2b'],
                    [1, '#3e3e40']
                ]
            },
            style: {
                fontFamily: "'Unica One', sans-serif"
            },
            plotBorderColor: '#606063'
        },
        title: {
            style: {
                color: '#E0E0E3',
                textTransform: 'uppercase',
                fontSize: '20px'
            }
        },
        subtitle: {
            style: {
                color: '#E0E0E3',
                textTransform: 'uppercase'
            }
        },
        xAxis: {
            gridLineColor: '#707073',
            labels: {
                style: {
                    color: '#E0E0E3'
                }
            },
            lineColor: '#707073',
            minorGridLineColor: '#505053',
            tickColor: '#707073',
            title: {
                style: {
                    color: '#A0A0A3'

                }
            }
        },
        yAxis: {
            gridLineColor: '#707073',
            labels: {
                style: {
                    color: '#E0E0E3'
                }
            },
            lineColor: '#707073',
            minorGridLineColor: '#505053',
            tickColor: '#707073',
            tickWidth: 1,
            title: {
                style: {
                    color: '#A0A0A3'
                }
            }
        },
        tooltip: {
            backgroundColor: 'rgba(0, 0, 0, 0.85)',
            style: {
                color: '#F0F0F0'
            }
        },
        plotOptions: {
            series: {
                dataLabels: {
                    color: '#B0B0B3'
                },
                marker: {
                    lineColor: '#333'
                }
            },
            boxplot: {
                fillColor: '#505053'
            },
            candlestick: {
                lineColor: 'white'
            },
            errorbar: {
                color: 'white'
            }
        },
        legend: {
            itemStyle: {
                color: '#E0E0E3'
            },
            itemHoverStyle: {
                color: '#FFF'
            },
            itemHiddenStyle: {
                color: '#606063'
            }
        },
        credits: {
            style: {
                color: '#666'
            }
        },
        labels: {
            style: {
                color: '#707073'
            }
        },

        drilldown: {
            activeAxisLabelStyle: {
                color: '#F0F0F3'
            },
            activeDataLabelStyle: {
                color: '#F0F0F3'
            }
        },

        navigation: {
            buttonOptions: {
                symbolStroke: '#DDDDDD',
                theme: {
                    fill: '#505053'
                }
            }
        },

        // scroll charts
        rangeSelector: {
            buttonTheme: {
                fill: '#505053',
                stroke: '#000000',
                style: {
                    color: '#CCC'
                },
                states: {
                    hover: {
                        fill: '#707073',
                        stroke: '#000000',
                        style: {
                            color: 'white'
                        }
                    },
                    select: {
                        fill: '#000003',
                        stroke: '#000000',
                        style: {
                            color: 'white'
                        }
                    }
                }
            },
            inputBoxBorderColor: '#505053',
            inputStyle: {
                backgroundColor: '#333',
                color: 'silver'
            },
            labelStyle: {
                color: 'silver'
            }
        },

        navigator: {
            handles: {
                backgroundColor: '#666',
                borderColor: '#AAA'
            },
            outlineColor: '#CCC',
            maskFill: 'rgba(255,255,255,0.1)',
            series: {
                color: '#7798BF',
                lineColor: '#A6C7ED'
            },
            xAxis: {
                gridLineColor: '#505053'
            }
        },

        scrollbar: {
            barBackgroundColor: '#808083',
            barBorderColor: '#808083',
            buttonArrowColor: '#CCC',
            buttonBackgroundColor: '#606063',
            buttonBorderColor: '#606063',
            rifleColor: '#FFF',
            trackBackgroundColor: '#404043',
            trackBorderColor: '#404043'
        },

        // special colors for some of the
        legendBackgroundColor: 'rgba(0, 0, 0, 0.5)',
        background2: '#505053',
        dataLabelsColor: '#B0B0B3',
        textColor: '#C0C0C0',
        contrastTextColor: '#F0F0F3',
        maskColor: 'rgba(255,255,255,0.3)'
    };

    // Apply the theme
    Highcharts.setOptions(Highcharts.theme);
}

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
    buildJobInfo();
    var cs = getCompletedStages();
    var ct = getCompletedTasks();
    linkCompletedTasksToStages();

    var sw = getShuffleWrite();
    var tSE = getTasksStartEnd();
    var bp = getBoxPlotData();
    var gs = getStagesTime();
    var gt = getTasksTimePerHost();
    loadStageGraph('#container', [cs, ct], 'Stages/Tasks Duration', 'Stage', 'Time (ms)', formatter)
    loadStageGraph('#shuffleWrite', [sw], 'ShuffleWrite Size', 'Stage', 'Byte', formatter)
    loadStageGraph('#tasksTime', [tSE], ' Task startTime endTime ', 'Task ID', 'Time (ms)', formatter, false)
    loadStageGraph('#boxplot', [bp], ' Task Execution Time per Stage ', 'Stage ID', 'Time (ms)', formatter, false)
    loadStageGraph('#pieStage', [gs], ' Stage Execution Time ', 'Host', 'Time (ms)', null, false)

    loadStageGraph('#pieHost', [gt], ' Task Execution Time per Host ', 'Host', 'Time (ms)', null, false)

    buildTable();


};

function buildJobInfo() {
    var jobInfo = filterEvent(allEvents, "SparkListenerEnvironmentUpdate")[0];
    var sparkProperties = jobInfo["Spark Properties"];
    var timeStart = new Date(filterEvent(allEvents, "SparkListenerApplicationStart")[0]["Timestamp"]);
    var info = {
        jar: sparkProperties["spark.jars"],
        app: sparkProperties["spark.app.name"],
        driver: sparkProperties["spark.driver.host"],
        master: sparkProperties["spark.master"],
        driverMemory: sparkProperties["spark.driver.memory"],
        executorMemory: sparkProperties["spark.executor.memory"],
    }
    var s = "";
    //  s+="<summary> Job Details </summary>";
    //  s+="<ul>";
    // s+="<li> Started on " + timeStart + "</li>";
    // s+="<li> Jar : " +  info.jar + "</li>";
    // s+="<li> App : "  + info.app  + "</li>";
    // s+="<li>Driver : " + info.driver + "</li>";
    // s+="<li> Master : " + info.master + "</li>";
    // s+="<li> Driver Memory " + info.driverMemory  + "</li>";
    // s+="<li> Executor Memory " + info.executorMemory  + "</li>";
    //s+="</ul></details>";
    s += "<b> Started on </b>" + timeStart + "<br>";
    s += "<b> Jar </b>" + info.jar + "<br>";
    s += "<b> App </b>" + info.app + "<br>";
    s += "<b> Driver </b>" + info.driver + "<br>";
    s += "<b> Master </b>" + info.master + "<br>";
    s += "<b> Driver Memory </b>" + info.driverMemory + "<br>";
    s += "<b> Executor Memory </b>" + info.executorMemory + "<br>";


    $('#jobInfo').html(s);
}


function getCompletedStages() {
    stageCompleted = filterEvent(allEvents, "SparkListenerStageCompleted");
    parsedStages = stageCompleted.map(function(x, index) {
        var info = x["Stage Info"]
        stageIndexMapping[info["Stage ID"]] = index;
        return {
            ctype: 'stage',
            x: index,
            y: info["Completion Time"] - info["Submission Time"],
            stage: {
                submissionTime : info["Submission Time"],
                completionTime : info["Completion Time"],
                duration: info["Completion Time"] - info["Submission Time"],
                stageID: info["Stage ID"],
                numTasks: info["Number of Tasks"],
                taskList: [],
                taskTime: []
            }
        }
    });
    return {
        type: "line",
        name: "Stages",
        // ctype: "stage",
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
        var updatedBlocks = metrics["Updated Blocks"]
        var blockMemory;
        if (updatedBlocks != undefined) {
            blockMemory = [];
            for (var i = 0; i < updatedBlocks.length; i++) {
                blockMemory.push(updatedBlocks[i]["Status"]["Memory Size"])
            }
        }
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
                taskID: info["Task ID"],
                stageID: x["Stage ID"],
                host: info["Host"],
                launchTime: info["Launch Time"],
                finishTime: info["Finish Time"],
                duration: info["Finish Time"] - info["Launch Time"],
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
                blockMemory: blockMemory,
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
            task: elem.task
        }
    });
    return {
        type: 'scatter',
        name: 'Shuffle Write',
        id: 2,
        color: 'rgba(68, 170, 213, 0.8)',
        data: shuffleWriteTasks
    }
}

function getTasksStartEnd() {
    taskStartEnd = parsedTasks.map(function(elem, index) {
        return {
            ctype: "task",
            x: elem.task.taskID,
            low: elem.task.launchTime,
            high: elem.task.finishTime,
            task: elem.task
        }
    });

    function compare(a, b) {
        return a.x - b.x;
    }
    taskStartEnd.sort(compare);
    //now that it is sorted, substract the start time of the first
    minTime = taskStartEnd[0].low;
    taskStartEnd.map(function(elem) {
        elem.low -= minTime;
        elem.high -= minTime;
    });


    return {
        type: 'columnrange',

        name: 'Task Start End',
        id: 3,
        color: 'rgba(68, 170, 213, 0.8)',
        data: taskStartEnd
    }
}

function getBoxPlotData() {
    boxplotData = parsedStages.map(function(elem) {
        var nums = stats(elem.stage.taskTime);
        return {
            ctype: "stage",
            x: elem.x,
            low: nums.min(),
            q1: nums.q1(),
            median: nums.median(),
            q3: nums.q3(),
            high: nums.max(),
            stage: elem.stage
        }
    });
    return {
        type: 'boxplot',
        name: 'Task execution time',
        id: 4,
        color: 'rgba(68, 170, 213, 0.8)',
        data: boxplotData

    }
}

//For pie drawing
function getTasksTimePerHost() {
    var hostTime = {}
    var total = 0;
    for (var i = 0; i < parsedTasks.length; i++) {
        var elem = parsedTasks[i];
        if (hostTime[elem.task.host] == undefined) {
            hostTime[elem.task.host] = 0;
        }
        hostTime[elem.task.host] += elem.task.duration;
        total += elem.task.duration;
    };
    var data = [];
    for (var index in hostTime) {
        if (!hostTime.hasOwnProperty(index)) {
            continue;
        }
        var f = Math.round(hostTime[index] * 100.0 / total, 2);
        data.push({
            name: index + " (" + f + " %)",
            y: hostTime[index]
        });
        //  console.log(index);
        // console.log(obj[index]);
    }
    // for (var i =0;i<hostTime.length;i++) {
    //     data.push({name});
    // }
    return {
        type: "pie",
        name: "Time per host",
        colorByPoint: true,
        data: data
    }
}
//For pie Drawing
function getStagesTime() {
    var stageTime = {}
    var total = 0;
    for (var i = 0; i < parsedStages.length; i++) {
        var elem = parsedStages[i];
        if (stageTime[elem.stage.stageID] == undefined) {
            stageTime[elem.stage.stageID] = 0;
        }
        stageTime[elem.stage.stageID] = elem.stage.duration;
        total += elem.stage.duration;
    }

    var data = [];
    for (var index in stageTime) {
        if (!stageTime.hasOwnProperty(index)) {
            continue;
        }
        var f = Math.round(stageTime[index] * 100.0 / total, 2);
        data.push({
            name: index + " (" + f + " %)",
            y: stageTime[index]
        });
    }
    return {
        type: "pie",
        name: "Time per stage",
        colorByPoint: true,
        data: data
    }
}

function buildTable() {
    var s = "<thead> <tr> " +
    "<th class=\"text-center sort-default\" style=\"width:10%\">Stage</th> " +
    "<th class=\"text-center col-sm-2\">Task</th>" +
    "<th class=\"text-center col-sm-2\">Host</th>"+
    "<th class=\"text-center col-sm-2\">Runtime</th>"+
    "<th class=\"text-center col-sm-2\">Launch Time</th>"+
    "<th class=\"text-center col-sm-2\">Finish Time</th>"+
    " </tr> </thead> "
    s+="<tbody>"

    var template = "{{#d}}<tr> {{#task}}" +
        "<td> {{stageID}}</td> " +
        "<td> {{taskID}}</td> " +
        "<td> {{host}}</td> " +
        "<td> {{runTime}}</td> " +
        "<td> {{logicTime launchTime}}</td> " +
        "<td> {{logicTime finishTime}}</td> " +
        "{{/task}}</tr>{{/d}}"

    // s += Mustache.render(template, {
    //     d: parsedTasks
    // });
//handler to shift time
Handlebars.registerHelper("logicTime", function(time) {
 return ""+ time-minTime;
});

var han = Handlebars.compile(template);
s+=han({d:parsedTasks});

     s+="</tbody>"

    $("#detailedTable").html(s);
        new Tablesort(document.getElementById('detailedTable'));

    return s;
}

function linkCompletedTasksToStages() {
    for (var i = 0; i < parsedTasks.length; i++) {
        var tID = parsedTasks[i].task.taskID;
        var tTime = parsedTasks[i].task.duration;
        var sID = parsedTasks[i].task.stageID;
        var index = stageIndexMapping[sID];
        parsedStages[index].stage.taskList.push(tID);
        parsedStages[index].stage.taskTime.push(tTime);
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
                inverted: inverted,
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
                    stickyTracking: false,

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
                formatter: formatter,
                shadow: true,
                hideDelay: 10
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
    //console.log(this.point.taskID)
    // console.log(this.y)
    s = '<b>Stage : ' + th.point.stage.stageID + '</b>';
    s += '<br> Num Tasks : ' + th.point.stage.numTasks;
    s += '<br> Duration : ' + th.point.y + ' ms';
    s += '<br> Tasks  : ' + th.point.stage.taskList;
    return s;
}


function taskFormatter(th) {
    //console.log(this.point.taskID)
    // console.log(this.y)
    s = '<b>Task : ' + th.point.task.taskID + '</b>';
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
        s = '<b>Task : ' + elem.taskID + '</b>';
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