#NSQ-Elastic

###Nsqelastic is the trial and implementation of nsqd cluster.

This project is hosted and opensourced by deepglint.com Muse Group.

##Design Principle:

The princaple of this to manage the resource layer by layer.

##Client Interchange Rules:

Nsq Elastic check the nsqd nodes round by round via nsqlookupd to get out the metrics for every node, and every topic/channel/client spreaded on the different nodes.

When A client wanna pub a message to the topic A, the message will be transfered to the nodes, hosts the topic A.

When A client wanna sub a topic, it should ask the Elastic with /api/sub url, tell Elastic channel it wanna sub, Elastic will check the datastructure, and judge the resources ,and return the node address at last.

Then client could access that nsqd node to sub the messages.


##Resources:

###Topic

The Topic are the set of messages which has same topic name, only 1 nsqd cannot host too many realtime topic,cause the constrain of the computer resources , even if theoretically unlimited
###Nsqd

The Nsqd Node could be seemed as a server or process node. Multi node could be the base of distributed cluster.

###Connection

Connection are meant the physic tcp connection, just like the information roads.

###Machine

Machines are the most basic resources for the elastic cluster. Like AWS EC2 there always provide api to apply or delete a physical machine.

So at the first we can get the features like below.

##Features:

###Topic Management

###Tcp Management

###Docker Management

###Machine Management
 


##Service:

###Elastic

Is the headquarter to control and judge everything.

###N2n

Is the management of tcp connections.

###DockerManager

Is the management for nsqd dockers.

###MachineManager

Is the management for physical machines.

###Client

The Client sdk to sub/pub messages.

##Maintainers

yanhuang@deepglint.com

zizhenyan@deepglint.com

PPT:[https://slides.com/yanhuang/deck-1]