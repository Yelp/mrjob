# -*- coding: utf-8 -*-
# Copyright 2009-2012 Yelp
# Copyright 2013 Yelp and Contributors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Utility functions for compatibility with different version of hadoop."""
from distutils.version import LooseVersion
import logging
import os

# lists alternative names for jobconf variables
# full listing thanks to translation table in
# http://hadoop.apache.org/common/docs/current/hadoop-project-dist/hadoop-common/DeprecatedProperties.html

log = logging.getLogger(__name__)

JOBCONF_DICT_LIST = [
    {'0.18': 'StorageId',
     '0.21': 'dfs.datanode.StorageId'},
    {'0.18': 'create.empty.dir.if.nonexist',
     '0.21': 'mapreduce.jobcontrol.createdir.ifnotexist'},
    {'0.18': 'dfs.access.time.precision',
     '0.21': 'dfs.namenode.accesstime.precision'},
    {'0.18': 'dfs.backup.address',
     '0.21': 'dfs.namenode.backup.address'},
    {'0.18': 'dfs.backup.http.address',
     '0.21': 'dfs.namenode.backup.http-address'},
    {'0.18': 'dfs.balance.bandwidthPerSec',
     '0.21': 'dfs.datanode.balance.bandwidthPerSec'},
    {'0.18': 'dfs.block.size',
     '0.21': 'dfs.blocksize'},
    {'0.18': 'dfs.client.buffer.dir',
     '0.21': 'fs.client.buffer.dir'},
    {'0.18': 'dfs.data.dir',
     '0.21': 'dfs.datanode.data.dir'},
    {'0.18': 'dfs.datanode.max.xcievers',
     '0.21': 'dfs.datanode.max.transfer.threads'},
    {'0.18': 'dfs.df.interval',
     '0.21': 'fs.df.interval'},
    {'0.18': 'dfs.http.address',
     '0.21': 'dfs.namenode.http-address'},
    {'0.18': 'dfs.https.address',
     '0.21': 'dfs.namenode.https-address'},
    {'0.18': 'dfs.https.client.keystore.resource',
     '0.21': 'dfs.client.https.keystore.resource'},
    {'0.18': 'dfs.https.need.client.auth',
     '0.21': 'dfs.client.https.need-auth'},
    {'0.18': 'dfs.max-repl-streams',
     '0.21': 'dfs.namenode.replication.max-streams'},
    {'0.18': 'dfs.max.objects',
     '0.21': 'dfs.namenode.max.objects'},
    {'0.18': 'dfs.name.dir',
     '0.21': 'dfs.namenode.name.dir'},
    {'0.18': 'dfs.name.dir.restore',
     '0.21': 'dfs.namenode.name.dir.restore'},
    {'0.18': 'dfs.name.edits.dir',
     '0.21': 'dfs.namenode.edits.dir'},
    {'0.18': 'dfs.permissions',
     '0.21': 'dfs.permissions.enabled'},
    {'0.18': 'dfs.permissions.supergroup',
     '0.21': 'dfs.permissions.superusergroup'},
    {'0.18': 'dfs.read.prefetch.size',
     '0.21': 'dfs.client.read.prefetch.size'},
    {'0.18': 'dfs.replication.considerLoad',
     '0.21': 'dfs.namenode.replication.considerLoad'},
    {'0.18': 'dfs.replication.interval',
     '0.21': 'dfs.namenode.replication.interval'},
    {'0.18': 'dfs.replication.min',
     '0.21': 'dfs.namenode.replication.min'},
    {'0.18': 'dfs.replication.pending.timeout.sec',
     '0.21': 'dfs.namenode.replication.pending.timeout-sec'},
    {'0.18': 'dfs.safemode.extension',
     '0.21': 'dfs.namenode.safemode.extension'},
    {'0.18': 'dfs.safemode.threshold.pct',
     '0.21': 'dfs.namenode.safemode.threshold-pct'},
    {'0.18': 'dfs.secondary.http.address',
     '0.21': 'dfs.namenode.secondary.http-address'},
    {'0.18': 'dfs.socket.timeout',
     '0.21': 'dfs.client.socket-timeout'},
    {'0.18': 'dfs.upgrade.permission',
     '0.21': 'dfs.namenode.upgrade.permission'},
    {'0.18': 'dfs.write.packet.size',
     '0.21': 'dfs.client-write-packet-size'},
    {'0.18': 'fs.checkpoint.dir',
     '0.21': 'dfs.namenode.checkpoint.dir'},
    {'0.18': 'fs.checkpoint.edits.dir',
     '0.21': 'dfs.namenode.checkpoint.edits.dir'},
    {'0.18': 'fs.checkpoint.period',
     '0.21': 'dfs.namenode.checkpoint.period'},
    {'0.18': 'fs.default.name',
     '0.21': 'fs.defaultFS'},
    {'0.18': 'hadoop.configured.node.mapping',
     '0.21': 'net.topology.configured.node.mapping'},
    {'0.18': 'hadoop.job.history.location',
     '0.21': 'mapreduce.jobtracker.jobhistory.location'},
    {'0.18': 'hadoop.native.lib',
     '0.21': 'io.native.lib.available'},
    {'0.18': 'hadoop.net.static.resolutions',
     '0.21': 'mapreduce.tasktracker.net.static.resolutions'},
    {'0.18': 'hadoop.pipes.command-file.keep',
     '0.21': 'mapreduce.pipes.commandfile.preserve'},
    {'0.18': 'hadoop.pipes.executable',
     '0.21': 'mapreduce.pipes.executable'},
    {'0.18': 'hadoop.pipes.executable.interpretor',
     '0.21': 'mapreduce.pipes.executable.interpretor'},
    {'0.18': 'hadoop.pipes.java.mapper',
     '0.21': 'mapreduce.pipes.isjavamapper'},
    {'0.18': 'hadoop.pipes.java.recordreader',
     '0.21': 'mapreduce.pipes.isjavarecordreader'},
    {'0.18': 'hadoop.pipes.java.recordwriter',
     '0.21': 'mapreduce.pipes.isjavarecordwriter'},
    {'0.18': 'hadoop.pipes.java.reducer',
     '0.21': 'mapreduce.pipes.isjavareducer'},
    {'0.18': 'hadoop.pipes.partitioner',
     '0.21': 'mapreduce.pipes.partitioner'},
    {'0.18': 'heartbeat.recheck.interval',
     '0.21': 'dfs.namenode.heartbeat.recheck-interval'},
    {'0.18': 'io.bytes.per.checksum',
     '0.21': 'dfs.bytes-per-checksum'},
    {'0.18': 'io.sort.factor',
     '0.21': 'mapreduce.task.io.sort.factor'},
    {'0.18': 'io.sort.mb',
     '0.21': 'mapreduce.task.io.sort.mb'},
    {'0.18': 'io.sort.spill.percent',
     '0.21': 'mapreduce.map.sort.spill.percent'},
    {'0.18': 'job.end.notification.url',
     '0.21': 'mapreduce.job.end-notification.url'},
    {'0.18': 'job.end.retry.attempts',
     '0.21': 'mapreduce.job.end-notification.retry.attempts'},
    {'0.18': 'job.end.retry.interval',
     '0.21': 'mapreduce.job.end-notification.retry.interval'},
    {'0.18': 'job.local.dir',
     '0.21': 'mapreduce.job.local.dir'},
    {'0.18': 'jobclient.completion.poll.interval',
     '0.21': 'mapreduce.client.completion.pollinterval'},
    {'0.18': 'jobclient.output.filter',
     '0.21': 'mapreduce.client.output.filter'},
    {'0.18': 'jobclient.progress.monitor.poll.interval',
     '0.21': 'mapreduce.client.progressmonitor.pollinterval'},
    {'0.18': 'keep.failed.task.files',
     '0.21': 'mapreduce.task.files.preserve.failedtasks'},
    {'0.18': 'keep.task.files.pattern',
     '0.21': 'mapreduce.task.files.preserve.filepattern'},
    {'0.18': 'key.value.separator.in.input.line',
     '0.21': 'mapreduce.input.keyvaluelinerecordreader.key.value.separator'},
    {'0.18': 'local.cache.size',
     '0.21': 'mapreduce.tasktracker.cache.local.size'},
    {'0.18': 'map.input.file',
     '0.21': 'mapreduce.map.input.file'},
    {'0.18': 'map.input.length',
     '0.21': 'mapreduce.map.input.length'},
    {'0.18': 'map.input.start',
     '0.21': 'mapreduce.map.input.start'},
    {'0.18': 'map.output.key.field.separator',
     '0.21': 'mapreduce.map.output.key.field.separator'},
    {'0.18': 'map.output.key.value.fields.spec',
     '0.21': 'mapreduce.fieldsel.map.output.key.value.fields.spec'},
    {'0.18': 'mapred.acls.enabled',
     '0.21': 'mapreduce.cluster.acls.enabled'},
    {'0.18': 'mapred.binary.partitioner.left.offset',
     '0.21': 'mapreduce.partition.binarypartitioner.left.offset'},
    {'0.18': 'mapred.binary.partitioner.right.offset',
     '0.21': 'mapreduce.partition.binarypartitioner.right.offset'},
    {'0.18': 'mapred.cache.archives',
     '0.21': 'mapreduce.job.cache.archives'},
    {'0.18': 'mapred.cache.archives.timestamps',
     '0.21': 'mapreduce.job.cache.archives.timestamps'},
    {'0.18': 'mapred.cache.files',
     '0.21': 'mapreduce.job.cache.files'},
    {'0.18': 'mapred.cache.files.timestamps',
     '0.21': 'mapreduce.job.cache.files.timestamps'},
    {'0.18': 'mapred.cache.localArchives',
     '0.21': 'mapreduce.job.cache.local.archives'},
    {'0.18': 'mapred.cache.localFiles',
     '0.21': 'mapreduce.job.cache.local.files'},
    {'0.18': 'mapred.child.tmp',
     '0.21': 'mapreduce.task.tmp.dir'},
    {'0.18': 'mapred.cluster.average.blacklist.threshold',
     '0.21': 'mapreduce.jobtracker.blacklist.average.threshold'},
    {'0.18': 'mapred.cluster.map.memory.mb',
     '0.21': 'mapreduce.cluster.mapmemory.mb'},
    {'0.18': 'mapred.cluster.max.map.memory.mb',
     '0.21': 'mapreduce.jobtracker.maxmapmemory.mb'},
    {'0.18': 'mapred.cluster.max.reduce.memory.mb',
     '0.21': 'mapreduce.jobtracker.maxreducememory.mb'},
    {'0.18': 'mapred.cluster.reduce.memory.mb',
     '0.21': 'mapreduce.cluster.reducememory.mb'},
    {'0.18': 'mapred.committer.job.setup.cleanup.needed',
     '0.21': 'mapreduce.job.committer.setup.cleanup.needed'},
    {'0.18': 'mapred.compress.map.output',
     '0.21': 'mapreduce.map.output.compress'},
    {'0.18': 'mapred.create.symlink',
     '0.21': 'mapreduce.job.cache.symlink.create'},
    {'0.18': 'mapred.data.field.separator',
     '0.21': 'mapreduce.fieldsel.data.field.separator'},
    {'0.18': 'mapred.debug.out.lines',
     '0.21': 'mapreduce.task.debugout.lines'},
    {'0.18': 'mapred.healthChecker.interval',
     '0.21': 'mapreduce.tasktracker.healthchecker.interval'},
    {'0.18': 'mapred.healthChecker.script.args',
     '0.21': 'mapreduce.tasktracker.healthchecker.script.args'},
    {'0.18': 'mapred.healthChecker.script.path',
     '0.21': 'mapreduce.tasktracker.healthchecker.script.path'},
    {'0.18': 'mapred.healthChecker.script.timeout',
     '0.21': 'mapreduce.tasktracker.healthchecker.script.timeout'},
    {'0.18': 'mapred.heartbeats.in.second',
     '0.21': 'mapreduce.jobtracker.heartbeats.in.second'},
    {'0.18': 'mapred.hosts',
     '0.21': 'mapreduce.jobtracker.hosts.filename'},
    {'0.18': 'mapred.hosts.exclude',
     '0.21': 'mapreduce.jobtracker.hosts.exclude.filename'},
    {'0.18': 'mapred.inmem.merge.threshold',
     '0.21': 'mapreduce.reduce.merge.inmem.threshold'},
    {'0.18': 'mapred.input.dir',
     '0.21': 'mapreduce.input.fileinputformat.inputdir'},
    {'0.18': 'mapred.input.dir.formats',
     '0.21': 'mapreduce.input.multipleinputs.dir.formats'},
    {'0.18': 'mapred.input.dir.mappers',
     '0.21': 'mapreduce.input.multipleinputs.dir.mappers'},
    {'0.18': 'mapred.input.pathFilter.class',
     '0.21': 'mapreduce.input.pathFilter.class'},
    {'0.18': 'mapred.jar',
     '0.21': 'mapreduce.job.jar'},
    {'0.18': 'mapred.job.classpath.archives',
     '0.21': 'mapreduce.job.classpath.archives'},
    {'0.18': 'mapred.job.classpath.files',
     '0.21': 'mapreduce.job.classpath.files'},
    {'0.18': 'mapred.job.id',
     '0.21': 'mapreduce.job.id'},
    {'0.18': 'mapred.job.map.memory.mb',
     '0.21': 'mapreduce.map.memory.mb'},
    {'0.18': 'mapred.job.name',
     '0.21': 'mapreduce.job.name'},
    {'0.18': 'mapred.job.priority',
     '0.21': 'mapreduce.job.priority'},
    {'0.18': 'mapred.job.queue.name',
     '0.21': 'mapreduce.job.queuename'},
    {'0.18': 'mapred.job.reduce.input.buffer.percent',
     '0.21': 'mapreduce.reduce.input.buffer.percent'},
    {'0.18': 'mapred.job.reduce.markreset.buffer.percent',
     '0.21': 'mapreduce.reduce.markreset.buffer.percent'},
    {'0.18': 'mapred.job.reduce.memory.mb',
     '0.21': 'mapreduce.reduce.memory.mb'},
    {'0.18': 'mapred.job.reduce.total.mem.bytes',
     '0.21': 'mapreduce.reduce.memory.totalbytes'},
    {'0.18': 'mapred.job.reuse.jvm.num.tasks',
     '0.21': 'mapreduce.job.jvm.numtasks'},
    {'0.18': 'mapred.job.shuffle.input.buffer.percent',
     '0.21': 'mapreduce.reduce.shuffle.input.buffer.percent'},
    {'0.18': 'mapred.job.shuffle.merge.percent',
     '0.21': 'mapreduce.reduce.shuffle.merge.percent'},
    {'0.18': 'mapred.job.tracker',
     '0.21': 'mapreduce.jobtracker.address'},
    {'0.18': 'mapred.job.tracker.handler.count',
     '0.21': 'mapreduce.jobtracker.handler.count'},
    {'0.18': 'mapred.job.tracker.history.completed.location',
     '0.21': 'mapreduce.jobtracker.jobhistory.completed.location'},
    {'0.18': 'mapred.job.tracker.http.address',
     '0.21': 'mapreduce.jobtracker.http.address'},
    {'0.18': 'mapred.job.tracker.jobhistory.lru.cache.size',
     '0.21': 'mapreduce.jobtracker.jobhistory.lru.cache.size'},
    {'0.18': 'mapred.job.tracker.persist.jobstatus.active',
     '0.21': 'mapreduce.jobtracker.persist.jobstatus.active'},
    {'0.18': 'mapred.job.tracker.persist.jobstatus.dir',
     '0.21': 'mapreduce.jobtracker.persist.jobstatus.dir'},
    {'0.18': 'mapred.job.tracker.persist.jobstatus.hours',
     '0.21': 'mapreduce.jobtracker.persist.jobstatus.hours'},
    {'0.18': 'mapred.job.tracker.retire.jobs',
     '0.21': 'mapreduce.jobtracker.retirejobs'},
    {'0.18': 'mapred.job.tracker.retiredjobs.cache.size',
     '0.21': 'mapreduce.jobtracker.retiredjobs.cache.size'},
    {'0.18': 'mapred.jobinit.threads',
     '0.21': 'mapreduce.jobtracker.jobinit.threads'},
    {'0.18': 'mapred.jobtracker.instrumentation',
     '0.21': 'mapreduce.jobtracker.instrumentation'},
    {'0.18': 'mapred.jobtracker.job.history.block.size',
     '0.21': 'mapreduce.jobtracker.jobhistory.block.size'},
    {'0.18': 'mapred.jobtracker.maxtasks.per.job',
     '0.21': 'mapreduce.jobtracker.maxtasks.perjob'},
    {'0.18': 'mapred.jobtracker.restart.recover',
     '0.21': 'mapreduce.jobtracker.restart.recover'},
    {'0.18': 'mapred.jobtracker.taskScheduler',
     '0.21': 'mapreduce.jobtracker.taskscheduler'},
    {'0.18': 'mapred.jobtracker.taskScheduler.maxRunningTasksPerJob',
     '0.21': 'mapreduce.jobtracker.taskscheduler.maxrunningtasks.perjob'},
    {'0.18': 'mapred.jobtracker.taskalloc.capacitypad',
     '0.21': 'mapreduce.jobtracker.taskscheduler.taskalloc.capacitypad'},
    {'0.18': 'mapred.join.expr',
     '0.21': 'mapreduce.join.expr'},
    {'0.18': 'mapred.join.keycomparator',
     '0.21': 'mapreduce.join.keycomparator'},
    {'0.18': 'mapred.lazy.output.format',
     '0.21': 'mapreduce.output.lazyoutputformat.outputformat'},
    {'0.18': 'mapred.line.input.format.linespermap',
     '0.21': 'mapreduce.input.lineinputformat.linespermap'},
    {'0.18': 'mapred.linerecordreader.maxlength',
     '0.21': 'mapreduce.input.linerecordreader.line.maxlength'},
    {'0.18': 'mapred.local.dir',
     '0.21': 'mapreduce.cluster.local.dir'},
    {'0.18': 'mapred.local.dir.minspacekill',
     '0.21': 'mapreduce.tasktracker.local.dir.minspacekill'},
    {'0.18': 'mapred.local.dir.minspacestart',
     '0.21': 'mapreduce.tasktracker.local.dir.minspacestart'},
    {'0.18': 'mapred.map.child.env',
     '0.21': 'mapreduce.map.env'},
    {'0.18': 'mapred.map.child.java.opts',
     '0.21': 'mapreduce.map.java.opts'},
    {'0.18': 'mapred.map.child.log.level',
     '0.21': 'mapreduce.map.log.level'},
    {'0.18': 'mapred.map.max.attempts',
     '0.21': 'mapreduce.map.maxattempts'},
    {'0.18': 'mapred.map.output.compression.codec',
     '0.21': 'mapreduce.map.output.compress.codec'},
    {'0.18': 'mapred.map.task.debug.script',
     '0.21': 'mapreduce.map.debug.script'},
    {'0.18': 'mapred.map.tasks',
     '0.21': 'mapreduce.job.maps'},
    {'0.18': 'mapred.map.tasks.speculative.execution',
     '0.21': 'mapreduce.map.speculative'},
    {'0.18': 'mapred.mapoutput.key.class',
     '0.21': 'mapreduce.map.output.key.class'},
    {'0.18': 'mapred.mapoutput.value.class',
     '0.21': 'mapreduce.map.output.value.class'},
    {'0.18': 'mapred.mapper.regex',
     '0.21': 'mapreduce.mapper.regex'},
    {'0.18': 'mapred.mapper.regex.group',
     '0.21': 'mapreduce.mapper.regexmapper..group'},
    {'0.18': 'mapred.max.map.failures.percent',
     '0.21': 'mapreduce.map.failures.maxpercent'},
    {'0.18': 'mapred.max.reduce.failures.percent',
     '0.21': 'mapreduce.reduce.failures.maxpercent'},
    {'0.18': 'mapred.max.split.size',
     '0.21': 'mapreduce.input.fileinputformat.split.maxsize'},
    {'0.18': 'mapred.max.tracker.blacklists',
     '0.21': 'mapreduce.jobtracker.tasktracker.maxblacklists'},
    {'0.18': 'mapred.max.tracker.failures',
     '0.21': 'mapreduce.job.maxtaskfailures.per.tracker'},
    {'0.18': 'mapred.merge.recordsBeforeProgress',
     '0.21': 'mapreduce.task.merge.progress.records'},
    {'0.18': 'mapred.min.split.size',
     '0.21': 'mapreduce.input.fileinputformat.split.minsize'},
    {'0.18': 'mapred.min.split.size.per.node',
     '0.21': 'mapreduce.input.fileinputformat.split.minsize.per.node'},
    {'0.18': 'mapred.min.split.size.per.rack',
     '0.21': 'mapreduce.input.fileinputformat.split.minsize.per.rack'},
    {'0.18': 'mapred.output.compress',
     '0.21': 'mapreduce.output.fileoutputformat.compress'},
    {'0.18': 'mapred.output.compression.codec',
     '0.21': 'mapreduce.output.fileoutputformat.compress.codec'},
    {'0.18': 'mapred.output.compression.type',
     '0.21': 'mapreduce.output.fileoutputformat.compress.type'},
    {'0.18': 'mapred.output.dir',
     '0.21': 'mapreduce.output.fileoutputformat.outputdir'},
    {'0.18': 'mapred.output.key.class',
     '0.21': 'mapreduce.job.output.key.class'},
    {'0.18': 'mapred.output.key.comparator.class',
     '0.21': 'mapreduce.job.output.key.comparator.class'},
    {'0.18': 'mapred.output.value.class',
     '0.21': 'mapreduce.job.output.value.class'},
    {'0.18': 'mapred.output.value.groupfn.class',
     '0.21': 'mapreduce.job.output.group.comparator.class'},
    {'0.18': 'mapred.permissions.supergroup',
     '0.21': 'mapreduce.cluster.permissions.supergroup'},
    {'0.18': 'mapred.pipes.user.inputformat',
     '0.21': 'mapreduce.pipes.inputformat'},
    {'0.18': 'mapred.reduce.child.env',
     '0.21': 'mapreduce.reduce.env'},
    {'0.18': 'mapred.reduce.child.java.opts',
     '0.21': 'mapreduce.reduce.java.opts'},
    {'0.18': 'mapred.reduce.child.log.level',
     '0.21': 'mapreduce.reduce.log.level'},
    {'0.18': 'mapred.reduce.max.attempts',
     '0.21': 'mapreduce.reduce.maxattempts'},
    {'0.18': 'mapred.reduce.parallel.copies',
     '0.21': 'mapreduce.reduce.shuffle.parallelcopies'},
    {'0.18': 'mapred.reduce.slowstart.completed.maps',
     '0.21': 'mapreduce.job.reduce.slowstart.completedmaps'},
    {'0.18': 'mapred.reduce.task.debug.script',
     '0.21': 'mapreduce.reduce.debug.script'},
    {'0.18': 'mapred.reduce.tasks',
     '0.21': 'mapreduce.job.reduces'},
    {'0.18': 'mapred.reduce.tasks.speculative.execution',
     '0.21': 'mapreduce.reduce.speculative'},
    {'0.18': 'mapred.seqbinary.output.key.class',
     '0.21': 'mapreduce.output.seqbinaryoutputformat.key.class'},
    {'0.18': 'mapred.seqbinary.output.value.class',
     '0.21': 'mapreduce.output.seqbinaryoutputformat.value.class'},
    {'0.18': 'mapred.shuffle.connect.timeout',
     '0.21': 'mapreduce.reduce.shuffle.connect.timeout'},
    {'0.18': 'mapred.shuffle.read.timeout',
     '0.21': 'mapreduce.reduce.shuffle.read.timeout'},
    {'0.18': 'mapred.skip.attempts.to.start.skipping',
     '0.21': 'mapreduce.task.skip.start.attempts'},
    {'0.18': 'mapred.skip.map.auto.incr.proc.count',
     '0.21': 'mapreduce.map.skip.proc-count.auto-incr'},
    {'0.18': 'mapred.skip.map.max.skip.records',
     '0.21': 'mapreduce.map.skip.maxrecords'},
    {'0.18': 'mapred.skip.on',
     '0.21': 'mapreduce.job.skiprecords'},
    {'0.18': 'mapred.skip.out.dir',
     '0.21': 'mapreduce.job.skip.outdir'},
    {'0.18': 'mapred.skip.reduce.auto.incr.proc.count',
     '0.21': 'mapreduce.reduce.skip.proc-count.auto-incr'},
    {'0.18': 'mapred.skip.reduce.max.skip.groups',
     '0.21': 'mapreduce.reduce.skip.maxgroups'},
    {'0.18': 'mapred.speculative.execution.slowNodeThreshold',
     '0.21': 'mapreduce.job.speculative.slownodethreshold'},
    {'0.18': 'mapred.speculative.execution.slowTaskThreshold',
     '0.21': 'mapreduce.job.speculative.slowtaskthreshold'},
    {'0.18': 'mapred.speculative.execution.speculativeCap',
     '0.21': 'mapreduce.job.speculative.speculativecap'},
    {'0.18': 'mapred.submit.replication',
     '0.21': 'mapreduce.client.submit.file.replication'},
    {'0.18': 'mapred.system.dir',
     '0.21': 'mapreduce.jobtracker.system.dir'},
    {'0.18': 'mapred.task.cache.levels',
     '0.21': 'mapreduce.jobtracker.taskcache.levels'},
    {'0.18': 'mapred.task.id',
     '0.21': 'mapreduce.task.attempt.id'},
    {'0.18': 'mapred.task.is.map',
     '0.21': 'mapreduce.task.ismap'},
    {'0.18': 'mapred.task.partition',
     '0.21': 'mapreduce.task.partition'},
    {'0.18': 'mapred.task.profile',
     '0.21': 'mapreduce.task.profile'},
    {'0.18': 'mapred.task.profile.maps',
     '0.21': 'mapreduce.task.profile.maps'},
    {'0.18': 'mapred.task.profile.params',
     '0.21': 'mapreduce.task.profile.params'},
    {'0.18': 'mapred.task.profile.reduces',
     '0.21': 'mapreduce.task.profile.reduces'},
    {'0.18': 'mapred.task.timeout',
     '0.21': 'mapreduce.task.timeout'},
    {'0.18': 'mapred.task.tracker.http.address',
     '0.21': 'mapreduce.tasktracker.http.address'},
    {'0.18': 'mapred.task.tracker.report.address',
     '0.21': 'mapreduce.tasktracker.report.address'},
    {'0.18': 'mapred.task.tracker.task-controller',
     '0.21': 'mapreduce.tasktracker.taskcontroller'},
    {'0.18': 'mapred.tasktracker.dns.interface',
     '0.21': 'mapreduce.tasktracker.dns.interface'},
    {'0.18': 'mapred.tasktracker.dns.nameserver',
     '0.21': 'mapreduce.tasktracker.dns.nameserver'},
    {'0.18': 'mapred.tasktracker.events.batchsize',
     '0.21': 'mapreduce.tasktracker.events.batchsize'},
    {'0.18': 'mapred.tasktracker.expiry.interval',
     '0.21': 'mapreduce.jobtracker.expire.trackers.interval'},
    {'0.18': 'mapred.tasktracker.indexcache.mb',
     '0.21': 'mapreduce.tasktracker.indexcache.mb'},
    {'0.18': 'mapred.tasktracker.instrumentation',
     '0.21': 'mapreduce.tasktracker.instrumentation'},
    {'0.18': 'mapred.tasktracker.map.tasks.maximum',
     '0.21': 'mapreduce.tasktracker.map.tasks.maximum'},
    {'0.18': 'mapred.tasktracker.memory_calculator_plugin',
     '0.21': 'mapreduce.tasktracker.resourcecalculatorplugin'},
    {'0.18': 'mapred.tasktracker.memorycalculatorplugin',
     '0.21': 'mapreduce.tasktracker.resourcecalculatorplugin'},
    {'0.18': 'mapred.tasktracker.reduce.tasks.maximum',
     '0.21': 'mapreduce.tasktracker.reduce.tasks.maximum'},
    {'0.18': 'mapred.tasktracker.taskmemorymanager.monitoring-interval',
     '0.21': 'mapreduce.tasktracker.taskmemorymanager.monitoringinterval'},
    {'0.18': 'mapred.tasktracker.tasks.sleeptime-before-sigkill',
     '0.21': 'mapreduce.tasktracker.tasks.sleeptimebeforesigkill'},
    {'0.18': 'mapred.temp.dir',
     '0.21': 'mapreduce.cluster.temp.dir'},
    {'0.18': 'mapred.text.key.comparator.options',
     '0.21': 'mapreduce.partition.keycomparator.options'},
    {'0.18': 'mapred.text.key.partitioner.options',
     '0.21': 'mapreduce.partition.keypartitioner.options'},
    {'0.18': 'mapred.textoutputformat.separator',
     '0.21': 'mapreduce.output.textoutputformat.separator'},
    {'0.18': 'mapred.tip.id',
     '0.21': 'mapreduce.task.id'},
    {'0.18': 'mapred.used.genericoptionsparser',
     '0.21': 'mapreduce.client.genericoptionsparser.used'},
    {'0.18': 'mapred.userlog.limit.kb',
     '0.21': 'mapreduce.task.userlog.limit.kb'},
    {'0.18': 'mapred.userlog.retain.hours',
     '0.21': 'mapreduce.job.userlog.retain.hours'},
    {'0.18': 'mapred.work.output.dir',
     '0.21': 'mapreduce.task.output.dir'},
    {'0.18': 'mapred.working.dir',
     '0.21': 'mapreduce.job.working.dir'},
    {'0.18': 'mapreduce.combine.class',
     '0.21': 'mapreduce.job.combine.class'},
    {'0.18': 'mapreduce.inputformat.class',
     '0.21': 'mapreduce.job.inputformat.class'},
    {'0.18': 'mapreduce.jobtracker.permissions.supergroup',
     '0.21': 'mapreduce.cluster.permissions.supergroup'},
    {'0.18': 'mapreduce.map.class',
     '0.21': 'mapreduce.job.map.class'},
    {'0.18': 'mapreduce.outputformat.class',
     '0.21': 'mapreduce.job.outputformat.class'},
    {'0.18': 'mapreduce.partitioner.class',
     '0.21': 'mapreduce.job.partitioner.class'},
    {'0.18': 'mapreduce.reduce.class',
     '0.21': 'mapreduce.job.reduce.class'},
    {'0.18': 'min.num.spills.for.combine',
     '0.21': 'mapreduce.map.combine.minspills'},
    {'0.18': 'reduce.output.key.value.fields.spec',
     '0.21': 'mapreduce.fieldsel.reduce.output.key.value.fields.spec'},
    {'0.18': 'security.job.submission.protocol.acl',
     '0.21': 'security.job.client.protocol.acl'},
    {'0.18': 'security.task.umbilical.protocol.acl',
     '0.21': 'security.job.task.protocol.acl'},
    {'0.18': 'sequencefile.filter.class',
     '0.21': 'mapreduce.input.sequencefileinputfilter.class'},
    {'0.18': 'sequencefile.filter.frequency',
     '0.21': 'mapreduce.input.sequencefileinputfilter.frequency'},
    {'0.18': 'sequencefile.filter.regex',
     '0.21': 'mapreduce.input.sequencefileinputfilter.regex'},
    {'0.18': 'session.id',
     '0.21': 'dfs.metrics.session-id'},
    {'0.18': 'slave.host.name',
     '0.21': 'dfs.datanode.hostname'},
    {'0.18': 'slave.host.name',
     '0.21': 'mapreduce.tasktracker.host.name'},
    {'0.18': 'tasktracker.contention.tracking',
     '0.21': 'mapreduce.tasktracker.contention.tracking'},
    {'0.18': 'tasktracker.http.threads',
     '0.21': 'mapreduce.tasktracker.http.threads'},
    {'0.18': 'topology.node.switch.mapping.impl',
     '0.21': 'net.topology.node.switch.mapping.impl'},
    {'0.18': 'topology.script.file.name',
     '0.21': 'net.topology.script.file.name'},
    {'0.18': 'topology.script.number.args',
     '0.21': 'net.topology.script.number.args'},
    {'0.18': 'user.name',
     '0.21': 'mapreduce.job.user.name'},
    {'0.18': 'webinterface.private.actions',
     '0.21': 'mapreduce.jobtracker.webinterface.trusted'},
]

# Issue #534: 1.x is the new 0.20, 2.x is the new 0.21+
for jobconf_dict in JOBCONF_DICT_LIST:
    jobconf_dict['1.0'] = jobconf_dict['0.18']
    jobconf_dict['2.0'] = jobconf_dict['0.21']


def _dict_list_to_compat_map(dict_list):
    # compat_map = {
    #   ...
    #   a: {'0.18': a, '0.21': b}
    #   b: {'0.18': a, '0.21': b}
    #   ..
    # }
    compat_map = {}
    for version_dict in dict_list:
        for value in version_dict.itervalues():
            compat_map[value] = version_dict
    return compat_map


_JOBCONF_MAP = _dict_list_to_compat_map(JOBCONF_DICT_LIST)


def jobconf_from_env(variable, default=None):
    """Get the value of a jobconf variable from the runtime environment.

    For example, a :py:class:`~mrjob.job.MRJob` could use
    ``jobconf_from_env('map.input.file')`` to get the name of the file a
    mapper is reading input from.

    If the name of the jobconf variable is different in different versions of
    Hadoop (e.g. in Hadoop 0.21, ``map.input.file`` is
    ``mapreduce.map.input.file``), we'll automatically try all variants before
    giving up.

    Return *default* if that jobconf variable isn't set.
    """
    # try variable verbatim first
    name = variable.replace('.', '_')
    if name in os.environ:
        return os.environ[name]

    # try alternatives (arbitrary order)
    for var in _JOBCONF_MAP.get(variable, {}).itervalues():
        name = var.replace('.', '_')
        if name in os.environ:
            return os.environ[name]

    return default

# old, deprecated name for get_jobconf_value().
get_jobconf_value = jobconf_from_env


def jobconf_from_dict(jobconf, name, default=None):
    """Get the value of a jobconf variable from the given dictionary.

    :param dict jobconf: jobconf dictionary
    :param string name: name of the jobconf variable (e.g. ``'user.name'``)
    :param default: fallback value

    If the name of the jobconf variable is different in different versions of
    Hadoop (e.g. in Hadoop 0.21, ``map.input.file`` is
    ``mapreduce.map.input.file``), we'll automatically try all variants before
    giving up.

    Return *default* if that jobconf variable isn't set.
    """
    if name in jobconf:
        return jobconf[name]

    # try alternatives (arbitrary order)
    for alternative in _JOBCONF_MAP.get(name, {}).itervalues():
        if alternative in jobconf:
            return jobconf[alternative]

    return default


def translate_jobconf(variable, version):
    """Translate *variable* to Hadoop version *version*. If it's not
    a variable we recognize, leave as-is.
    """
    if not variable in _JOBCONF_MAP:
        return variable

    req_version = LooseVersion(version)
    possible_versions = sorted(_JOBCONF_MAP[variable].keys(),
                               reverse=True,
                               key=lambda(v): LooseVersion(v))

    for possible_version in possible_versions:
        if req_version >= LooseVersion(possible_version):
            return _JOBCONF_MAP[variable][possible_version]

    # return oldest version if we don't find required version
    return _JOBCONF_MAP[variable][possible_versions[-1]]


def supports_combiners_in_hadoop_streaming(version):
    """Return ``True`` if this version of Hadoop Streaming supports combiners
    (i.e. >= 0.20.203), otherwise False.
    """
    return version_gte(version, '0.20')


def supports_new_distributed_cache_options(version):
    """Use ``-files`` and ``-archives`` instead of ``-cacheFile`` and
    ``-cacheArchive``
    """
    # Although Hadoop 0.20 supports these options, that support is buggy:
    # https://issues.apache.org/jira/browse/MAPREDUCE-2361
    # https://issues.apache.org/jira/browse/HADOOP-6334
    # The bug was fixed in Hadoop 0.20.203.0:
    # http://hadoop.apache.org/common/docs/r0.20.203.0/releasenotes.html
    return version_gte(version, '0.20.203')


def uses_020_counters(version):
    return version_gte(version, '0.20')


def uses_generic_jobconf(version):
    """Use ``-D`` instead of ``-jobconf``"""
    return version_gte(version, '0.20')


def version_gte(version, cmp_version_str):
    """Return ``True`` if version >= *cmp_version_str*."""

    if not isinstance(version, basestring):
        raise TypeError('%r is not a string' % version)

    if not isinstance(cmp_version_str, basestring):
        raise TypeError('%r is not a string' % cmp_version_str)

    return LooseVersion(version) >= LooseVersion(cmp_version_str)


def add_translated_jobconf_for_hadoop_version(jobconf, hadoop_version):
    """Translates the configuration property name to match those that
    are accepted in hadoop_version. Prints a warning message if any
    configuration property name does not match the name in the hadoop
    version. Combines the original jobconf with the translated jobconf.

    :return: a map consisting of the original and translated configuration
             property names and values.
    """
    translated_jobconf = {}
    mismatch_key_to_translated_key = {}
    for key, value in jobconf.iteritems():
        new_key = translate_jobconf(key, hadoop_version)
        if key != new_key:
            translated_jobconf[new_key] = value
            mismatch_key_to_translated_key[key] = new_key

    if mismatch_key_to_translated_key:
        log.warning("Detected hadoop configuration property names that"
                    " do not match hadoop version %s:"
                    "\nThe have been translated as follows\n %s",
                    hadoop_version,
                    '\n'.join(["%s: %s" % (key, value) for key, value
                               in mismatch_key_to_translated_key.iteritems()]))

    translated_jobconf.update(jobconf)
    return translated_jobconf
