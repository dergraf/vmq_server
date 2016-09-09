<cluster>
<h3 class="ui center aligned header">Cluster Overview</h3>
<div class="ui two column stackable grid container">
    <div id="cluster-ring" class="column">
        <canvas id="vmq-cluster-queues" height="400" width="400"></canvas>
        <canvas id="vmq-cluster-messages" height="350" width="350"></canvas>
    </div>
    <div class="column">
        <table class="ui selectable inverted table">
            <thead>
                <tr>
                    <th></th>
                    <th>Name</th>
                    <th>Status</th>
                    <th class="right aligned">Notes</th>
                </tr>
            </thead>
            <tbody>
                <tr each={ nodes }>
                    <td style="background-color: {dark_color}"></td>
                    <td>{ name }</td>

                </tr>
            </tbody>
        </table>

        <div class="ui attached message">
            <div class="header">
                Add a new Cluster Node
            </div>
        </div>
        <div class="ui attached fluid segment">
            <div class="content">
                <p>Configure and boot up a new node with a unique nodename (in <code>vernemq.conf</code>)</p>
                <div class="ui small fluid input">
                    <input name="nodeNameInput" onkeyup={ edit } placeholder="MyVerneMQ@192.168.1.123" type="text">
                </div>

            </div>
        </div>
        <div onclick={ join } class="ui basic mini bottom attached button">
            <i class="meta fa fa-plus" aria-hidden="true"></i>
            Join Cluster
        </div>
    </div>
</div>

<style scoped>
    div#cluster_ring {
        position: relative;
        height: 400px;
        width: 400px;
    }
    canvas#vmq-cluster-queues, canvas#vmq-cluster-messages {
        position: absolute;
    }
    canvas#vmq-cluster-queues {
        top: 0px;
        left: 0px;
    }
    canvas#vmq-cluster-messages {
        border-radius: 150px;
        top: 25px;
        left: 25px;
        //top: 50%;
        // left: 50%;
        // transform: translate(-50%, -50%);
    }
</style>

<script>
this.cluster = {}
this.nodes = []

this.dark_colors = [
    '#024bfd',
    '#94fd02',
    '#fd02de',
    '#02fdd3',
    '#fd8a02',
    '#4102fd',
    '#0dfd02',
    '#fd0256',
    '#029ffd',
    '#e8fd02',
    '#c802fd',
    '#02fd7f',
    '#fd3602',
    '#0217fd',
    '#61fd02',
    '#fd02aa',
    '#02f3fd',
    '#fdbe02',
    '#7402fd',
    '#02fd2b',
    '#fd0222',
    '#026bfd',
    '#b5fd02',
    '#fc02fd',
    '#02fdb3',
    '#fd6a02',
    ]
this.light_colors = [
    '#b1c7fd',
    '#ddfdb1',
    '#fdb1f3',
    '#b1fdf0',
    '#fddab1',
    '#c4b1fd',
    '#b4fdb1',
    '#fdb1ca',
    '#b1e1fd',
    '#f7fdb1',
    '#edb1fd',
    '#b1fdd7',
    '#fdc1b1',
    '#b1b7fd',
    '#cefdb1',
    '#fdb1e4',
    '#b1fafd',
    '#fdeab1',
    '#d4b1fd',
    '#b1fdbd',
    '#fdb1bb',
    '#b1d1fd',
    '#e7fdb1',
    '#fdb1fd',
    '#b1fde7',
    '#fdd0b1',
    ]
//function rainbow(numOfSteps, step) {
//    // This function generates vibrant, "evenly spaced" colours (i.e. no clustering). This is ideal for creating easily distinguishable vibrant markers in Google Maps and other apps.
//    // Adam Cole, 2011-Sept-14
//    // HSV to RBG adapted from: http://mjijackson.com/2008/02/rgb-to-hsl-and-rgb-to-hsv-color-model-conversion-algorithms-in-javascript
//    var r, g, b;
//    var h = step / numOfSteps;
//    var i = ~~(h * 6);
//    var f = h * 6 - i;
//    var q = 1 - f;
//    switch(i % 6){
//        case 0: r = 1; g = f; b = 0; break;
//        case 1: r = q; g = 1; b = 0; break;
//        case 2: r = 0; g = 1; b = f; break;
//        case 3: r = 0; g = q; b = 1; break;
//        case 4: r = f; g = 0; b = 1; break;
//        case 5: r = 1; g = 0; b = q; break;
//    }
//    var c = "#" + ("00" + (~ ~(r * 255)).toString(16)).slice(-2) + ("00" + (~ ~(g * 255)).toString(16)).slice(-2) + ("00" + (~ ~(b * 255)).toString(16)).slice(-2);
//    return (c);
//}


var self = this

this.add_node = function(node) {
    if (typeof this.cluster[node] === 'undefined') {
        riot.compile(function() {
            var tag = riot.mount('cluster-node')
            self.cluster[node] = tag
            var id = Math.abs(node.hashCode())
            self.nodes.push({
                id: id, 
                name: node,
                dark_color: self.dark_colors[id % self.dark_colors.length],
                light_color: self.light_colors[id % self.light_colors.length],
            })
            self.update()
        })
    }
}
RiotControl.on('cluster', function(node) {
    self.add_node(node)
})

edit(e) {
    this.newNode = e.target.value
}
join(e) {
    if (this.newNode) {
        self.add_node(this.newNode)
    }
}

self.queue_data = {
    labels: [],
    datasets: [
    {data: [], backgroundColor: [], borderWidth: 0}
    ],
}
self.message_data = {
    labels: [],
    datasets: [
    {data: [], backgroundColor: [], borderWidth: 0}
    ],
}

self.old_msg_totals = {}
self.convert_to_rates = function(node, msg_in, msg_out, msg_drop) {

    if (typeof self.old_msg_totals[node] === 'undefined') {
        self.old_msg_totals[node] = 
            [0, 0, 0, Math.round(Date.now() / 1000)]
    }

    var now = Math.round(Date.now() / 1000)
    var diff = now - self.old_msg_totals[node][3]

    var msg_in_rate = Math.round((msg_in - self.old_msg_totals[node][0]) / diff)
    var msg_out_rate = Math.round((msg_out - self.old_msg_totals[node][1]) / diff)
    var msg_drop_rate = Math.round((msg_drop - self.old_msg_totals[node][2]) / diff)
    self.old_msg_totals[node] = [msg_in, msg_out, msg_drop, now]
    return [msg_in_rate, msg_out_rate, msg_drop_rate]
}

self.transform_metrics = function(metrics) {
    var queue_data = self.queue_data.datasets[0].data
    var queue_bgcolor = self.queue_data.datasets[0].backgroundColor
    var queue_labels = self.queue_data.labels
    queue_data.length = 0
    queue_bgcolor.length = 0
    queue_labels.length = 0
    var message_data = self.message_data.datasets[0].data
    var message_bgcolor = self.message_data.datasets[0].backgroundColor
    var message_labels = self.message_data.labels
    message_data.length = 0
    message_bgcolor.length = 0
    message_labels.length = 0
    for (var i in self.nodes) {
        var node = self.nodes[i]
        var node_metrics = metrics[node.name]
        var online_sessions = node_metrics.socket_open - node_metrics.socket_close
        var offline_sessions = node_metrics.queue_processes - online_sessions
        queue_data.push(online_sessions)
        queue_data.push(offline_sessions)
        queue_bgcolor.push(node.dark_color)
        queue_bgcolor.push(node.light_color)
        queue_labels.push(node.name + ' - Queues online')
        queue_labels.push(node.name + ' - Queues offline')

        var message_in = node_metrics.queue_message_in
        var message_out = node_metrics.queue_message_out
        var message_drop = node_metrics.queue_message_drop
        var rates = self.convert_to_rates(node.name, message_in, message_out, message_drop)
        message_data.push(rates[0])
        message_data.push(rates[1])
        message_data.push(rates[2])
        message_bgcolor.push(node.dark_color)
        message_bgcolor.push(node.light_color)
        message_bgcolor.push('red')
        message_labels.push(node.name + ' - Publish In')
        message_labels.push(node.name + ' - Publish Out')
        message_labels.push(node.name + ' - Publish Drop')
    }
    //self.queue_data.labels = queue_labels
    //self.queue_data.datasets[0].data = queue_data
    //self.queue_data.datasets[0].backgroundColors = queue_bgcolor

    //self.message_data.labels = message_labels
    //self.message_data.datasets[0].data = message_data
    //self.message_data.datasets[0].backgroundColors = message_bgcolor
}

self.on('updated', function() {
    var ctx_queues = $('#vmq-cluster-queues')
    if (typeof self.chart_queues === 'undefined') {
        self.chart_queues = Chart.Doughnut(ctx_queues, {
            data: self.queue_data,
            options: {
                cutoutPercentage: 90,
                responsive: false,
                title: {
                    display: false,
                },
                legend: {
                    display: false
                }
            }
        });
    }
    var ctx_messages = $('#vmq-cluster-messages')
    if (typeof self.chart_messages === 'undefined') {
        self.chart_messages = Chart.Doughnut(ctx_messages, {
            data: self.message_data,
            options: {
                cutoutPercentage: 90,
                responsive: false,
                title: {
                    display: false,
                },
                legend: {
                    display: false
                }
            }
        });
    }

})
self.on('mount', function() {
    RiotControl.on('cluster-metrics', function(data) {
        var metrics = data.metrics
        self.transform_metrics(metrics)
        if (typeof self.chart_queues !== 'undefined') {
            self.chart_queues.update()
        }
        if (typeof self.chart_messages !== 'undefined') {
            self.chart_messages.update()
        }
        self.update()
    })
    //RiotControl.on(self.name, function(data) {
    //    var metrics = data.metrics
    //    var online_sessions = metrics.socket_open - metrics.socket_close
    //    var offline_sessions = metrics.queue_processes - online_sessions
    //    var queue_in = metrics.queue_message_in
    //    var queue_out = metrics.queue_message_out
    //    var queue_drop = metrics.queue_message_drop
    //    self.change_rates(queue_in, queue_out, queue_drop)
    //    self.change_data(online_sessions, offline_sessions)
    //    var cpu_usage = metrics.system_utilization
    //    console.log(cpu_usage)
    //    self.change_cpu(cpu_usage)
    //    self.update()
    //})
})


</script>
</cluster>
