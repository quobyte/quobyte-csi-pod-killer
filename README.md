# Quobyte CSI Pod Killer

Kills application pods with Quobyte volumes if the Quobyte client is restarted (for example, on update or during crash)

It is shipped as a sidecar container as part of [Quobyte CSI](https://github.com/quobyte/quobyte-csi) and deployed on
each k8s node.

## Quobyte CSI Pod Killer Options

| Option | Type | Default | Description |
| :--- | :---: | :---: | :--- |
| monitoring_interval | string | 5s | Monitoring interval to find stale mounts |
| node_name | string |  | K8S hostname |
| service_url | string |  | Pod killer cache service URL |
| pod_lookup_batch_size | int | 10 | Batch size for pod uid resolution |
| parallel_kills | int | 10 | Kill 'n' pods with stale mount points |
| role | string | cache or monitor | Process role |

## Developer Notes

To Build container and push it, use:

```bash
./build container <version> #v1.0.0
```