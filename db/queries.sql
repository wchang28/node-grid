USE [Tools]
GO

/****** Object:  Table [dbo].[grid_job_tasks]    Script Date: 7/20/2015 8:36:49 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

SET ANSI_PADDING ON
GO

CREATE TABLE [dbo].[grid_job_tasks](
	[job_id] [bigint] NOT NULL,
	[index] [bigint] NOT NULL,
	[cmd] [varchar](max) NOT NULL,
	[cookie] [varchar](250) NULL,
	[stdin_file] [varchar](500) NULL,
	[status] [varchar](50) NOT NULL,
	[node] [varchar](250) NULL,
	[pid] [int] NULL,
	[start_time] [datetime] NULL,
	[finish_time] [datetime] NULL,
	[duration_seconds] [bigint] NULL,
	[success] [bit] NULL,
	[ret_code] [int] NULL,
	[aborted] [bit] NOT NULL,
	[stdout] [text] NULL,
	[stderr] [text] NULL,
 CONSTRAINT [PK_Table_1] PRIMARY KEY CLUSTERED 
(
	[job_id] ASC,
	[index] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]

GO

SET ANSI_PADDING ON
GO

USE [Tools]
GO

/****** Object:  Table [dbo].[grid_jobs]    Script Date: 7/20/2015 8:48:37 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

SET ANSI_PADDING ON
GO

CREATE TABLE [dbo].[grid_jobs](
	[job_id] [bigint] IDENTITY(1,1) NOT NULL,
	[description] [varchar](250) NULL,
	[cookie] [varchar](250) NULL,
	[user_id] [int] NOT NULL,
	[submit_time] [datetime] NOT NULL,
	[aborted] [bit] NOT NULL,
 CONSTRAINT [PK_grid_jobs] PRIMARY KEY CLUSTERED 
(
	[job_id] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]

GO

SET ANSI_PADDING ON
GO

USE [Tools]
GO

/****** Object:  View [dbo].[grid_jobs_view]    Script Date: 7/20/2015 8:37:36 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO




CREATE view [dbo].[grid_jobs_view]
as
with
stat
as
(
	select
	[job_id]
	,[num_tasks]=count(*)
	,[start_time]=min([start_time])
	,[max_finish_time]=max([finish_time])
	,[num_tasks_queued]=cast(sum(iif([status]='QUEUED',1,0)) as bigint)
	,[num_tasks_finished]=cast(sum(iif([status]='FINISHED',1,0)) as bigint)
	,[num_success]=cast(sum(iif([success] is null, 0, [success])) as bigint)
	from [dbo].[grid_job_tasks] (nolock)
	group by [job_id]
)
,stat2
as
(
	select
	jobs.[description]
	,jobs.[cookie]
	,jobs.[user_id]
	,jobs.[submit_time]
	,[status]=iif(jobs.[aborted]=1,'ABORTED',iif(stat.[num_tasks]=stat.[num_tasks_finished],'FINISHED', iif(stat.[num_tasks]=stat.[num_tasks_queued], 'SUBMITTED', 'STARTED')))
	,stat.*
	from stat
	left join [dbo].[grid_jobs] (nolock) jobs
	on stat.[job_id]=jobs.[job_id]
)
select
stat2.[job_id]
,stat2.[description]
,stat2.[cookie]
,stat2.[user_id]
,stat2.[submit_time]
,stat2.[status]
,stat2.[num_tasks]
,stat2.[num_tasks_finished]
,stat2.[start_time]
,[finish_time]=iif(stat2.[status]='FINISHED', stat2.[max_finish_time], null)
,[duration_seconds]=iif(stat2.[status]='FINISHED', DATEDIFF(s, stat2.[start_time], stat2.[max_finish_time]), null)
,[success]=cast(iif(stat2.[status]='ABORTED', 0, iif(stat2.[num_tasks]=stat2.[num_success],1,0)) as bit)
,[complete_pct]=cast(stat2.[num_tasks_finished] as float)/cast(stat2.[num_tasks] as float) * 100.0
from stat2;


GO

USE [Tools]
GO

/****** Object:  StoredProcedure [dbo].[stp_NodeJSGridAbortJob]    Script Date: 7/20/2015 8:38:40 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO




CREATE PROCEDURE [dbo].[stp_NodeJSGridAbortJob]
	@job_id bigint
AS
BEGIN
	SET NOCOUNT ON;

	update [dbo].[grid_jobs]
	set
	[aborted]=1
	where
	[job_id]=@job_id
	and [aborted]=0

	update [dbo].[grid_job_tasks]
	set
	[aborted]=1
	where [job_id]=@job_id
	and [aborted]=0

	exec [dbo].[stp_NodeJSGridGetJobRunningTasks] @job_id=@job_id
END

GO

USE [Tools]
GO

/****** Object:  StoredProcedure [dbo].[stp_NodeJSGridGetJobResult]    Script Date: 7/20/2015 8:39:17 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO



CREATE PROCEDURE [dbo].[stp_NodeJSGridGetJobResult]
	@job_id bigint
AS
BEGIN
	SET NOCOUNT ON;

	select
	[index]
	,[cookie]
	,[ret_code]
	,[stdout]
	,[stderr]
	from [dbo].[grid_job_tasks] (nolock)
	where [job_id] = @job_id
	order by [index] asc

END


GO

USE [Tools]
GO

/****** Object:  StoredProcedure [dbo].[stp_NodeJSGridGetJobRunningTasks]    Script Date: 7/20/2015 8:39:34 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO




CREATE PROCEDURE [dbo].[stp_NodeJSGridGetJobRunningTasks]
	@job_id bigint
AS
BEGIN
	SET NOCOUNT ON;
	
    select
	[job_id]
	,[index]
	,[node]
	,[pid]
	from [dbo].[grid_job_tasks] (nolock)
	where
	[job_id]=@job_id
	and [status]='RUNNING'
END



GO

USE [Tools]
GO

/****** Object:  StoredProcedure [dbo].[stp_NodeJSGridGetJobStatus]    Script Date: 7/20/2015 8:39:48 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO




CREATE PROCEDURE [dbo].[stp_NodeJSGridGetJobStatus]
	@job_id bigint
AS
BEGIN
	SET NOCOUNT ON;

	select
	*
	from [dbo].[grid_jobs_view] (nolock)
	where 
	[job_id]=@job_id

END



GO

USE [Tools]
GO

/****** Object:  StoredProcedure [dbo].[stp_NodeJSGridJobTask]    Script Date: 7/20/2015 8:39:59 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO




CREATE PROCEDURE [dbo].[stp_NodeJSGridJobTask]
	@job_id bigint
	,@task_index bigint
	,@node varchar(250) = null
	,@pid int = null
	,@ret_code int = null
	,@stdout text = null
	,@stderr text = null
	,@return_outputs bit = 0
AS
BEGIN

	SET NOCOUNT ON;

	if not @node is null -- dispatched
	begin
		-- QUEUED --> DISPATCHED
		update [dbo].[grid_job_tasks]
		set
		[node]=@node
		,[status]='DISPATCHED'
		where
		[job_id]=@job_id
		and [index]=@task_index
		and [status]='IDLE'
	end
	else
	begin
		if @ret_code is null -- mark task start
		begin
			-- DISPATCHED --> RUNNING
			update [dbo].[grid_job_tasks]
			set
			[pid]=@pid
			,[start_time]=getdate()
			,[status]='RUNNING'
			where
			[job_id]=@job_id
			and [index]=@task_index
			and [status]='DISPATCHED'
		end
		else
		begin -- mark task end
			-- RUNNING --> FINISHED
			declare @finish_time datetime
			set @finish_time=getdate()

			update [dbo].[grid_job_tasks]
			set
			[pid]=@pid
			,[status]='FINISHED'
			,[finish_time]=@finish_time
			,[duration_seconds]=iif([start_time] is null, 0, DATEDIFF(s, [start_time], @finish_time))
			,[success]=iif(@ret_code=0, 1, 0)
			,[ret_code]=@ret_code
			,[stdout]=@stdout
			,[stderr]=@stderr
			where
			[job_id]=@job_id
			and [index]=@task_index

			update [dbo].[grid_job_tasks]
			set
			[start_time]=@finish_time
			where
			[job_id]=@job_id
			and [index]=@task_index
			and [start_time] is null

		end -- mark task finished
	end -- mark task finished
		
	if @return_outputs=1
	begin
		select
		[cmd]
		,[cookie]
		,[stdin_file]
		,[status]
		,[node]
		,[pid]
		,[start_time]
		,[finish_time]
		,[duration_seconds]
		,[success]
		,[ret_code]
		,[aborted]
		,[stdout]
		,[stderr]
		from [dbo].[grid_job_tasks] (nolock)
		where [job_id]=@job_id and [index]=@task_index
	end
	else
	begin
		select
		[cmd]
		,[cookie]
		,[stdin_file]
		,[status]
		,[node]
		,[pid]
		,[start_time]
		,[finish_time]
		,[duration_seconds]
		,[success]
		,[ret_code]
		,[aborted]
		from [dbo].[grid_job_tasks] (nolock)
		where [job_id]=@job_id and [index]=@task_index
	end
END



GO

USE [Tools]
GO

/****** Object:  StoredProcedure [dbo].[stp_NodeJSGridSubmitJob]    Script Date: 7/20/2015 8:40:16 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO




CREATE PROCEDURE [dbo].[stp_NodeJSGridSubmitJob]
	-- Add the parameters for the stored procedure here
	@job_xml text
AS
BEGIN
	SET NOCOUNT ON;
	
	declare @xml xml
	set @xml=@job_xml
	declare @description varchar(250)
	declare @cookie varchar(250)
	declare @job_id bigint

	-- TODO:
	---------------------------------
	declare @user_id int
	set @user_id=0
	---------------------------------

	select
	@description=a.b.value('@description', 'varchar(250)')
	,@cookie=a.b.value('@cookie', 'varchar(250)') 
	FROM @xml.nodes('/job') a(b)
	
	insert into [dbo].[grid_jobs]
	([description],[cookie],[user_id],[submit_time],[aborted])
	values (@description, @cookie, @user_id, getdate(), 0)

	set @job_id = @@IDENTITY

	declare @tmp table
	(
		[id] bigint identity(0,1)
		,[cmd] varchar(max)
		,[cookie] varchar(256)
	)

	insert into @tmp
	([cmd],[cookie])
	select
	[cmd]=a.b.value('@cmd', 'varchar(max)')
	,[cookie]=a.b.value('@cookie', 'varchar(250)') 
	FROM @xml.nodes('/job/tasks/task') a(b)

	delete from [dbo].[grid_job_tasks] where [job_id]=@job_id

	insert into [dbo].[grid_job_tasks]
	([job_id],[index],[cmd],[cookie],[status],[aborted])
	select
	[job_id]=@job_id
	,[index]=[id]
	,[cmd]
	,[cookie]
	,[status]='IDLE'
	,[aborted]=0
	from @tmp

	select [job_id]=@job_id
END



GO