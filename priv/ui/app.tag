<app>

<main>


<cluster></cluster>
</main>
<script>
var metrics = riot.observable()
RiotControl.addStore(metrics)

metrics.fetch = function() {
    var cmd = {
        command: 'metrics/show',
        flags: {'all': [], 'table': []}
    }
    $.postJSON('/api', cmd, function(data) {
        var cluster = {}
        data.table.forEach(function(row) {
            if (typeof cluster[row.Node] === 'undefined') {
                cluster[row.Node] = {}
            }
            cluster[row.Node][row.Metric] = row.Value
        })
        RiotControl.trigger('cluster-metrics', {'metrics': cluster})
        for (var node in cluster) {
            RiotControl.trigger('cluster', node)
            RiotControl.trigger(node, {'metrics': cluster[node]})
        }
    })
}

var timer = setInterval(metrics.fetch, 5000)

this.on('unmount', function() {
    clearInterval(timer)
})
</script>
</app>
