-- timestamp2 for ms precision
CREATE TABLE T_MINIPAS_SYNC (
    ID BIGINT NOT NULL IDENTITY PRIMARY KEY,
    IDNUMMER VARCHAR(200) NOT NULL,
    SKEMAOPDAT_MS BIGINT,
    SOURCE_TABLE_NAME VARCHAR(50)
)

CREATE TABLE MinipasImporterStatus (
    ID BIGINT NOT NULL IDENTITY PRIMARY KEY,
    Type VARCHAR(20),
    StartTime DATETIME NOT NULL,
    EndTime DATETIME,
    Outcome VARCHAR(20),
    ErrorMessage VARCHAR(200)
)

// t_bes
USE [HAIBA_LPR_REPLIKA]
GO

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

SET ANSI_PADDING ON
GO

CREATE TABLE TRIFORK_MINIPAS.[t_bes](
	[V_RECNUM] [varchar](38) NULL,
	[D_AMBDTO] [datetime] NULL,
	[INDBERETNINGSDATO] [datetime] NULL
) ON [PRIMARY]

GO

SET ANSI_PADDING OFF
GO


CREATE TABLE [TRIFORK_MINIPAS].[t_adm](
	[contact_identification_id] [bigint] IDENTITY(1,1) NOT NULL,
	[c_sgh] [varchar](4) NOT NULL,
	[c_afd] [varchar](3) NOT NULL,
	[c_pattype] [varchar](1) NOT NULL,
	[d_inddto] [datetime] NOT NULL,
	[d_uddto] [datetime] NULL,
	[v_recnum] [varchar](255) NOT NULL,
	[d_importdto] [datetime] NULL,
	[v_status] [nvarchar](10) NULL,
	[v_cpr] [char](10) NULL,
	[c_indm] [varchar](50) NULL
) ON [PRIMARY]



