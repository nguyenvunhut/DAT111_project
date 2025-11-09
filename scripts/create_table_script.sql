USE master;
GO

-- Kiểm tra và tạo database HeartDisease
IF NOT EXISTS (SELECT name FROM sys.databases WHERE name = N'HeartDiseaseDB')
BEGIN
    CREATE DATABASE HeartDisease;
END
GO

USE HeartDiseaseDB;
GO

-- Bảng DIMPERSON: Nhân khẩu học (Giới tính, tuổi, chủng tộc)
CREATE TABLE dbo.DimPerson (
    PersonID INT IDENTITY(1,1) PRIMARY KEY CLUSTERED, 
    -- Khóa Chính (Surrogate Key), CLUSTERED để tối ưu JOIN

    Sex VARCHAR(10) NOT NULL,                           -- Giới tính: 'Male', 'Female'
    AgeCategory NVARCHAR(20) NOT NULL,                  -- Nhóm tuổi: 'Age 18–24', 'Age 25–29', ...
    RaceEthnicityCategory NVARCHAR(50) NOT NULL         -- Chủng tộc/dân tộc: 'White', 'Black', 'Hispanic', ...
);

-- Bảng DIMSTATE: Vùng địa lý
CREATE TABLE dbo.DimState (
    StateID INT IDENTITY(1,1) PRIMARY KEY CLUSTERED,    -- Khóa Chính

    StateName NVARCHAR(100) NOT NULL,                   -- Tên tiểu bang/vùng
    Region VARCHAR(50) NULL                             -- Vùng địa lý
);

-- Bảng DIMCHECKUPTIME: Thời điểm kiểm tra sức khỏe
CREATE TABLE dbo.DimCheckupTime (
    CheckupTimeID INT IDENTITY(1,1) PRIMARY KEY CLUSTERED, -- Khóa Chính

    LastCheckupTime VARCHAR(100) NOT NULL,               -- Thời điểm khám: 'Within past year', '5+ years ago', ...
    CheckupRecency INT NOT NULL                         -- Mức độ gần đây (định lượng)
);

-- Bảng DIMPHYSICALACTIVITY: Hoạt động thể chất
CREATE TABLE dbo.DimPhysicalActivity (
    PhysicalActivityID INT IDENTITY(1,1) PRIMARY KEY CLUSTERED, -- Khóa Chính

    PhysicalActivities VARCHAR(10) NOT NULL,            -- Có hoạt động thể chất: 'Yes'/'No'
    ActivityLevel VARCHAR(20) NOT NULL                  -- Mức độ vận động: 'Active'/'Inactive'
);

-- Bảng DIMLIFESTYLE: Hành vi lối sống
CREATE TABLE dbo.DimLifestyle (
    LifestyleID INT IDENTITY(1,1) PRIMARY KEY CLUSTERED, -- Khóa Chính

    SmokerStatus VARCHAR(30) NOT NULL,                   -- Tình trạng hút thuốc
    ECigaretteUsage VARCHAR(30) NOT NULL,                -- Tình trạng sử dụng vape
    AlcoholDrinkers VARCHAR(10) NOT NULL,                -- Uống rượu: 'Yes'/'No'
    SleepQuality VARCHAR(20) NOT NULL                    -- Chất lượng giấc ngủ: 'Poor', 'Good', ...
);

-- Bảng DIMCHRONICDISEASES: Bệnh mãn tính khác
CREATE TABLE dbo.DimChronicDiseases (
    ChronicDiseaseID INT IDENTITY(1,1) PRIMARY KEY CLUSTERED, -- Khóa Chính

    HadDiabetes VARCHAR(3) NOT NULL,                     -- Tiểu đường: 'Yes'/'No'
    HadArthritis VARCHAR(3) NOT NULL,                    -- Viêm khớp: 'Yes'/'No'
    HadCOPD VARCHAR(3) NOT NULL,                         -- Bệnh phổi mãn tính: 'Yes'/'No'
    HadKidneyDisease VARCHAR(3) NOT NULL,                -- Bệnh thận: 'Yes'/'No'
    HadDepressiveDisorder VARCHAR(3) NOT NULL,           -- Rối loạn trầm cảm: 'Yes'/'No'
    HadCancer VARCHAR(3) NOT NULL,                       -- Ung thư: 'Yes'/'No'
    HadSkinCancer VARCHAR(3) NOT NULL                    -- Ung thư da: 'Yes'/'No'
);

-- Bảng FACTHealthRecord: Hồ sơ sức khỏe tổng hợp
CREATE TABLE dbo.HealthRecord (
    HealthRecordID INT IDENTITY(1,1) NOT NULL,          -- Khóa Chính (Surrogate Key)

    PersonID INT NOT NULL,                              -- FK → DimPerson
    StateID INT NOT NULL,                               -- FK → DimState
    CheckupTimeID INT NOT NULL,                         -- FK → DimCheckupTime
    PhysicalActivityID INT NOT NULL,                    -- FK → DimPhysicalActivity
    ChronicDiseaseID INT NOT NULL,                      -- FK → DimChronicDiseases
    LifestyleID INT NOT NULL,                           -- FK → DimLifestyle

    HeartDiseaseFlag BIT NOT NULL,                      -- Cờ bệnh tim: 1/0
    PhysicalHealthDays DECIMAL(10, 4) NULL,             -- Số ngày sức khỏe thể chất kém
    MentalHealthDays DECIMAL(10, 4) NULL,               -- Số ngày sức khỏe tinh thần kém
    SleepHours DECIMAL(10, 4) NULL,                     -- Giờ ngủ trung bình
    HeightInMeters DECIMAL(10, 4) NULL,                 -- Chiều cao (m)
    WeightInKilograms DECIMAL(10, 4) NULL,              -- Cân nặng (kg)
    BMI DECIMAL(10, 4) NULL,                            -- Chỉ số BMI
    RecordYear INT NULL,                                -- Năm khảo sát

    CONSTRAINT PK_HealthRecord PRIMARY KEY NONCLUSTERED (HealthRecordID),
    -- Định nghĩa các Khóa Ngoại (FKs)
    CONSTRAINT FK_HealthRecord_Person FOREIGN KEY (PersonID) 
        REFERENCES dbo.DimPerson(PersonID),
    CONSTRAINT FK_HealthRecord_State FOREIGN KEY (StateID) 
        REFERENCES dbo.DimState(StateID),
    CONSTRAINT FK_HealthRecord_CheckupTime FOREIGN KEY (CheckupTimeID) 
        REFERENCES dbo.DimCheckupTime(CheckupTimeID),
    CONSTRAINT FK_HealthRecord_PhysicalActivity FOREIGN KEY (PhysicalActivityID) 
        REFERENCES dbo.DimPhysicalActivity(PhysicalActivityID),
    CONSTRAINT FK_HealthRecord_Disease FOREIGN KEY (ChronicDiseaseID) 
        REFERENCES dbo.DimChronicDiseases(ChronicDiseaseID),
    
    CONSTRAINT FK_HealthRecord_DimLifestyle FOREIGN KEY (LifestyleID) 
        REFERENCES dbo.DimLifestyle(LifestyleID)
);

-- Index CLUSTERED trên các Khóa Ngoại
CREATE CLUSTERED INDEX IX_HealthRecord_FKs ON dbo.HealthRecord (PersonID, StateID, CheckupTimeID);