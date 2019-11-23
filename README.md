<h1>GoConfigure</h1>
A simple application built on golang that can be used to configure multiple servers at the same time using SSH.
<br>
It has two parts:
<h3>1) Server</h3>
A server is an application deployed on a remote machine. It works as a master to control and configure remote machines. It tracks the output of the command executed and can follow log trails. It uses badgerDB tp store server credentials and outputs.
Server can be controlled by a client using RPC.

<h3>2) Client</h3>
Client is used to communicate with server. It can issue RPC commands to add new slave machines, read outputs and control server operations.


<h3>Technologies Used:</h3>
<li> Golang </li>
<li> gRPC </li>
<li> BadgerDB </li>
