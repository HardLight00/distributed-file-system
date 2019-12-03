# DFS: Distributed File System

## Components
<ol>
<li>Client</li>
<li>Master node</li>
<li>Data node</li>
</ol>

### Auxiliary components
<ol>
<li>Tree of directories</li>
<li>Abstract node</li>
</ol>

## Client
<ul>
<li> create [filename] - create a new empty file</li>
<li> read [filename]- unit all chunks and save them as a file </li>
<li> write [filename] - separate file into chunks and store them in data nodes</li>
<li> delete [filename] - delete file from system </li>
<li> info [filename] - return took size of file and number of chunks </li>
<li> copy [filename] [directory] - make a copy of file in directory </li>
<li> move [filename] [directory] - move file in directory </li>
<li> cd [directory] - open the directory. '.' - current directory, '..' - parent</li>
<li> ls [directory] - get list of all files in directory. With flag '-a' will show also sub directories</li>
<li> mk [directory] - make new directory</li>
<li> rm [directory] - delete all files and sub directories inside the directory</li>
</ul>

## Name node
### File structure
Stores files in a tree structure. 
Every node has a link to parent node, list of children nodes and absolute path in dfs

### File distribution
Also contain dictionary ('directory' -> 'files'), 
dictionary ('file' -> 'chunks'), dictionary ('chunk' -> 'replicas'). 
Every replica can be found by 'host' and 'port'. For every replica we also store a state - active or not. 
Checks the state using heartbeats every 5 sec.

## Data node
A flat structure of files with names as chunks and encoded data inside 



