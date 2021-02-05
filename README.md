# Quobyte CSI Pod Killer

Kills application pods with Quobyte volumes if the Quobyte client is restarted (for example, on update or during crash)

It is shipped as a sidecar container as part of [Quobyte CSI](https://github.com/quobyte/quobyte-csi) and deployed on
each k8s node.
