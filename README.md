cnidaria
===

cnidaria is a computing framework for writing distributed data structures
and algorithms

Description
---

cnidaria is a lightweight, decentralized distributed computing framework that 
allows users to implement non-embarrassingly parallel and embarrassingly 
parallel algorithms on a cluster of machines. The framework provides 
elasticity allowing new computing resources to be added or removed from the 
cluster "on the fly." New resources are incorporated into a running calculation 
immediately, dynamically "scaling up." cnidaria is also fault-tolerant and
resources can be removed at any time and the calculation will continue to run, 
albeit slighly slower.

In addition cnidaria provides distributed data structures. Currently 
vectors are supported with data.frames under active development. 
There are plans to include (sparse) matrices and associated numerical 
linear algebra routines. There are also plans to add roll-back functionality.

cnidaria is currently implemented in R only. However, there is nothing
R-specific about the architecture and the cnidaria approach is a general
strategy designed to address a comprehensive class of distributed computing
challenges.

Architectural Overview
---

cnidaria goes beyond the traditional master-slave architecture 
on which traditional non-embarrassingly parallel distributed computing
packages are base. Instead a *coordinator* broadcasts a computation that
will be consumed by workers. The coordinator does not interact directly 
with workers. Instead it sends computational tasks to a set of queues
that map to data blocks and blocks on a queue where the results are returned.

An individual worker blocks on a series of queues the correspond to the
data blocks that it serves. When a task involving its data block is 
received in a queue it takes the task off of the queue consumes the
task by carrying out the computation described by the task and returning
the result to a queue that is specified in the task.

This approach provides two advantages over traditional packages. First, 
there is no need to register new workers with a master. Adding new
computational resources to the cluster is accomplished by starting a 
new worker and having it block on an appropriate queue. If a computation
needs to be scaled out then a new worker simply copies existing 
resources that are being heavily used. Thus, the new worker decreases
the computing time by allowing the heavily used resource to be more 
readily available. Second, resources can easily be removed without
ruining the computation. If a worker is removed while it is serving a
task then the result it not pushed to the return queue where the
coordinator is waiting. This task eventually times out and the coordinator
resubmits the task. As long as there is a worker serving the needed 
resource it is correctly returned. If it is not available then the 
computation times out again and the coordinator signals that task could
note be completed. By providing process control through queues elasticity
is provided in a way that is simple and robust.

The approach also provides a simple solution for non-embarrassingly parallel
computing challenges. Roughly these problems can be characterized as
requiring a greater degree of communication between workers than those
required by map-reduce type operations.

Installing cnidaria
---

A simple example
---

Support
---

1. cnidaria is tested on Linux and Mac OS X.
2. The development home for this project can be found at: [https://github.com/kaneplusplus/cnidaria](https://github.com/kaneplusplus/cnidaria)
3. Contributions are welcome.
4. For more information contact Michael Kane at [kaneplusplus@gmail.com](kaneplusplus@gmail.com)
