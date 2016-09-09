<cluster-node-chart>
    <div class="ui stackable grid">
        <div class="row">
            <div class="eight wide column">
                <canvas height="200" class="chart-clients"></canvas>
            </div>
            <div class="eight wide column">
                <div class="ui labels">
                    <div class="ui mini teal label">online clients
                        <div class="detail">
                            { data.datasets[0].data[0] }
                        </div>
                    </div>
                    <div class="ui mini purple label">offline clients
                        <div class="detail">
                            { data.datasets[0].data[1] }
                        </div>
                    </div>
                    <div class="ui mini blue label">publish in / sec
                        <div class="detail">
                            { data.datasets[1].data[0] }
                        </div>
                    </div>
                    <div class="ui mini violet label">publish out / sec
                        <div class="detail">
                            { data.datasets[1].data[1] }
                        </div>
                    </div>
                    <div class="ui mini red label">publish drop / sec
                        <div class="detail">
                            { data.datasets[1].data[2] }
                        </div>
                    </div>
                </div>
            </div>
        </div>

        <div class="row">
            <div class="eight wide column">
                <div class="ui labels">
                    <div class="ui mini teal label">CPU utilization
                        <div class="detail">
                            { sysdata.datasets[0].data[0] } %
                        </div>
                    </div>
                </div>
            </div>
            <div class="eight wide column">
                <canvas height="200" class="chart-cpu"></canvas>
            </div>
        </div>
    </div>

    <script>

var self = this

self.data = {
    labels: [],
    datasets: [{
        label: ["online", "offline"],
        data: [0, 0],
        backgroundColor: [
            'teal',
            'purple',
        ],
    },{
        label: ["in", "out", "drop"],
        data: [0, 0, 0],
        backgroundColor: [
            'blue',
            'violet',
            'red'
        ],
    
    }]
}

self.sysdata = {
    labels: [],
    datasets: [{
        data: [0, 100],
        backgroundColor: [
            'teal',
            'grey',
        ],
    },{
        data: [0],
        backgroundColor: [
            'blue',
        ],
    
    }]
}

self.last_update = Math.round(Date.now() / 1000)
self.msg_in = 0
self.msg_out = 0
self.msg_drop = 0

this.change_data = function(online, offline) {
    self.data.datasets[0].data[0] = online
    self.data.datasets[0].data[1] = offline
}

this.change_rates = function(msg_in, msg_out, msg_drop) {
    var now = Math.round(Date.now() / 1000)
    var diff = now - self.last_update
    self.last_update = now
    console.log(msg_in, msg_out, msg_drop)
    var msg_in_rate = Math.round((msg_in - self.msg_in) / diff)
    var msg_out_rate = Math.round((msg_out - self.msg_out) / diff)
    var msg_drop_rate = Math.round((msg_drop - self.msg_drop) / diff)
    self.data.datasets[1].data[0] = msg_in_rate
    self.data.datasets[1].data[1] = msg_out_rate
    self.data.datasets[1].data[2] = msg_drop_rate
    self.msg_in = msg_in
    self.msg_out = msg_out
    self.msg_drop = msg_drop
}

this.change_cpu = function(utilization) {
    self.sysdata.datasets[0].data[0] = utilization
    self.sysdata.datasets[0].data[1] = 100 - utilization
}

self.on('updated', function() {
    var ctx_clients = $('#vmq-chart-'+this.id+' canvas.chart-clients')
    if (typeof self.chart_clients === 'undefined') {
        self.chart_clients = Chart.Doughnut(ctx_clients, {
            data: self.data,
            options: {
                responsive: true,
                title: {
                    display: false,
                },
                legend: {
                    display: false
                }
            }
        });
    }
    var ctx_cpu = $('#vmq-chart-'+this.id+' canvas.chart-cpu')
    if (typeof self.chart_cpu === 'undefined') {
        self.chart_cpu = Chart.Doughnut(ctx_cpu, {
            data: self.sysdata,
            options: {
                responsive: true,
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
    console.log("mount")
    RiotControl.on(self.name, function(data) {
        var metrics = data.metrics
        var online_sessions = metrics.socket_open - metrics.socket_close
        var offline_sessions = metrics.queue_processes - online_sessions
        var queue_in = metrics.queue_message_in
        var queue_out = metrics.queue_message_out
        var queue_drop = metrics.queue_message_drop
        self.change_rates(queue_in, queue_out, queue_drop)
        self.change_data(online_sessions, offline_sessions)
        var cpu_usage = metrics.system_utilization
        console.log(cpu_usage)
        self.change_cpu(cpu_usage)
        if (typeof self.chart_clients !== 'undefined') {
            self.chart_clients.update()
        }
        if (typeof self.chart_cpu !== 'undefined') {
            self.chart_cpu.update()
        }
        self.update()
    })
})
</script>
</cluster-node-chart>
