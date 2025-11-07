USE DAT111ProjectHeartDiseaseDB;
GO

-- 1. DEFINE VARIABLES
DECLARE @FilePath NVARCHAR(255) = 'D:\Learning\FPT_polytechnic\Sem4\DAT111\DAT111_project\data\processed\final_data.csv';
DECLARE @TableName NVARCHAR(100) = 'HeartDiseaseRaw';
DECLARE @Schema NVARCHAR(10) = 'dbo';

DECLARE @Message NVARCHAR(500);
DECLARE @SQL NVARCHAR(MAX);

-- 2. DROP EXISTING TABLE (Phần này để đảm bảo việc tạo lại bảng với cấu trúc mới)
SET @SQL = N'IF OBJECT_ID(''' + @Schema + N'.' + @TableName + N''') IS NOT NULL DROP TABLE ' + @Schema + N'.' + @TableName + N';';
EXEC sp_executesql @SQL;
PRINT N'Đã xóa bảng cũ (nếu có).';

-- 2.1 CREATE TABLE (Giữ nguyên cấu trúc đã sửa)
SET @SQL = N'
CREATE TABLE ' + @Schema + N'.' + @TableName + N' (
    State NVARCHAR(50),
    Sex NVARCHAR(10),
    GeneralHealth NVARCHAR(30),
    PhysicalHealthDays FLOAT,
    MentalHealthDays FLOAT,
    LastCheckupTime NVARCHAR(150),
    PhysicalActivities NVARCHAR(5),
    SleepHours FLOAT,
    RemovedTeeth NVARCHAR(50),
    HadHeartAttack NVARCHAR(5),
    HadAngina NVARCHAR(5),
    HadStroke NVARCHAR(5),
    HadAsthma NVARCHAR(5),
    HadSkinCancer NVARCHAR(5),
    HadCOPD NVARCHAR(5),
    HadDepressiveDisorder NVARCHAR(5),
    HadKidneyDisease NVARCHAR(5),
    HadArthritis NVARCHAR(5),
    HadDiabetes NVARCHAR(100),
    DeafOrHardOfHearing NVARCHAR(5),
    BlindOrVisionDifficulty NVARCHAR(5),
    DifficultyConcentrating NVARCHAR(5),
    DifficultyWalking NVARCHAR(3),
    DifficultyDressingBathing NVARCHAR(3),
    DifficultyErrands NVARCHAR(5),
    SmokerStatus NVARCHAR(100),
    ECigaretteUsage NVARCHAR(150),
    ChestScan NVARCHAR(5),
    RaceEthnicityCategory NVARCHAR(60),
    AgeCategory NVARCHAR(60),
    HeightInMeters DECIMAL(4,2),
    WeightInKilograms DECIMAL(5,2),
    BMI DECIMAL(5,2),
    AlcoholDrinkers NVARCHAR(3),
    HIVTesting NVARCHAR(3),
    FluVaxLast12 NVARCHAR(3),
    PneumoVaxEver NVARCHAR(3),
    TetanusLast10Tdap NVARCHAR(150),
    HighRiskLastYear NVARCHAR(5),
    CovidPos NVARCHAR(MAX)
);
';
EXEC sp_executesql @SQL;
PRINT N'Đã tạo bảng mới thành công.';


-- 3. BULK INSERT (SỬ DỤNG FORMAT = 'CSV')
SET @Message = N'Bắt đầu BULK INSERT...';
PRINT @Message;
SET @SQL = '
BULK INSERT ' + @Schema + N'.' + @TableName + '
FROM ''' + @FilePath + N'''
WITH (
    -- FORMAT = ''CSV'' tự động xử lý dấu ngoặc kép (Field Qualifiers)
    FORMAT = ''CSV'', 
    FIRSTROW = 2,
    -- Giữ ROWTERMINATOR và FIELDTERMINATOR cho FORMAT = ''CSV'' là không cần thiết
    -- nhưng nên giữ lại CODEPAGE = ''65001'' cho dữ liệu UTF-8
    CODEPAGE = ''65001''
);
';
EXEC sp_executesql @SQL;
PRINT N'BULK INSERT đã được thực thi thành công.';

-- 4. VERIFY THE IMPORT
SET @SQL = N'SELECT TOP 10 * FROM ' + @Schema + N'.' + @TableName + ';';
EXEC sp_executesql @SQL;

SET @SQL = N'SELECT COUNT(*) AS TotalRowsImported FROM ' + @Schema + N'.' + @TableName + ';';
EXEC sp_executesql @SQL;