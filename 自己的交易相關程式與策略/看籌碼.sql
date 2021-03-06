/****** SSMS 中 SelectTopNRows 命令的指令碼  ******/
SELECT [權證代號]
      ,[權證名稱]
      ,[券商代號]
      ,[券商名稱]
      ,sum([張增減]) as [張增減]
  FROM [warrant].[dbo].[BranchBSData]
  where 權證代號 = '4402' and 日期 between '20170503' and '20170517'
  group by [權證代號],[權證名稱],[券商代號],[券商名稱]
  order by [張增減] desc
