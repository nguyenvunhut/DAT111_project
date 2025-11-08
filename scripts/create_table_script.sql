USE DAT111ProjectHeartDiseaseDB;
GO

-- Tạo bảng FACT chính lưu trữ thông tin sức khỏe tổng hợp
CREATE TABLE dbo.HealthRecord (
    HealthRecordID INT IDENTITY(1,1) PRIMARY KEY,       -- Khóa chính, tự tăng

    PersonID INT NOT NULL,                              -- FK → DimPerson: thông tin cá nhân
    StateID INT NOT NULL,                               -- FK → DimState: nơi cư trú
    CheckupTimeID INT NOT NULL,                         -- FK → DimTime: thời điểm khám
    PhysicalActivityID INT NOT NULL,                    -- FK → DimPhysicalActivity: hoạt động thể chất
    ChronicDiseaseID INT NOT NULL,                      -- FK → DimChronicDiseases: bệnh mãn tính
    LifestyleID INT NOT NULL,                           -- FK → LifestyleID: lối sống

    HeartDiseaseFlag BIT NOT NULL,                      -- 1 nếu có bệnh tim, 0 nếu không
    PhysicalHealthDays FLOAT NULL,                      -- Số ngày sức khỏe Thể chất kém trong (30 ngày qua)
    MentalHealthDays FLOAT NULL,                        -- Số ngày sức khỏe Tinh thần kém (30 ngày qua)
    SleepHours FLOAT NULL,                              -- Số giờ ngủ trung bình mỗi ngày
    HeightInMeters FLOAT NULL,                          -- Chiều cao (m)
    WeightInKilograms FLOAT NULL,                       -- Cân nặng (kg)
    BMI FLOAT NULL,                                     -- Chỉ số khối cơ thể
    RecordYear INT NULL                                 -- Năm khảo sát (tùy chọn)

    CONSTRAINT FK_HealthRecord_Person FOREIGN KEY (PersonID) 
        REFERENCES dbo.DimPerson(PersonID),
    CONSTRAINT FK_HealthRecord_State FOREIGN KEY (StateID) 
        REFERENCES dbo.DimState(StateID),
    CONSTRAINT FK_HealthRecord_CheckupTime FOREIGN KEY (CheckupTimeID) 
        REFERENCES dbo.DimTime(CheckupTimeID),
    CONSTRAINT FK_HealthRecord_PhysicalActivity FOREIGN KEY (PhysicalActivityID) 
        REFERENCES dbo.DimPhysicalActivity(PhysicalActivityID),
    CONSTRAINT FK_HealthRecord_Disease FOREIGN KEY (ChronicDiseaseID) 
        REFERENCES dbo.DimChronicDiseases(ChronicDiseaseID),
    CONSTRAINT FK_HealthRecord_DimLifestyle FOREIGN KEY (LifestyleID) 
        REFERENCES dbo.DimLifestyle(LifestyleID),
);

-- Tạo bảng DIMPERSON: lưu thông tin nhân khẩu học
CREATE TABLE dbo.DimPerson (
    PersonID INT IDENTITY(1,1) PRIMARY KEY,             -- Khóa chính

    Sex VARCHAR(10) NOT NULL,                           -- Giới tính: Male/Female
    AgeCategory VARCHAR(20) NOT NULL,                   -- Nhóm tuổi: Age 18–24, Age 25–29, ...
    RaceEthnicityCategory VARCHAR(50) NOT NULL          -- Chủng tộc/dân tộc: White, Black, Hispanic, ...
);

-- Tạo bảng DIMSTATE: lưu thông tin vùng địa lý
CREATE TABLE dbo.DimState (
    StateID INT IDENTITY(1,1) PRIMARY KEY,              -- Khóa chính

    StateName VARCHAR(100) NOT NULL,                    -- Tên tiểu bang/vùng
    Region VARCHAR(50) NULL                             -- Vùng địa lý: Northeast, Midwest, South, West
);

-- Tạo bảng DIMCHECKUPTIME: lưu thông tin thời điểm kiểm tra sức khỏe
CREATE TABLE dbo.DimCheckupTime (
    CheckupTimeID INT IDENTITY(1,1) PRIMARY KEY,        -- Khóa chính

    LastCheckupTime VARCHAR(50) NOT NULL,               -- Thời điểm khám: Within past year, 5+ years ago, ...
    CheckupRecency INT NOT NULL                         -- Mức độ gần đây: 1 = gần nhất, 4 = lâu nhất
);

-- Tạo bảng DIMPHYSICALACTIVITY: lưu thông tin hoạt động thể chất
CREATE TABLE dbo.DimPhysicalActivity (
    PhysicalActivityID INT IDENTITY(1,1) PRIMARY KEY,   -- Khóa chính

    PhysicalActivities VARCHAR(10) NOT NULL,            -- Có hoạt động thể chất: Yes/No
    ActivityLevel VARCHAR(20) NOT NULL                  -- Mức độ vận động: Active/Inactive
);

-- Tạo bảng DIMLIFESTYLE: lưu thông tin hành vi lối sống
CREATE TABLE dbo.DimLifestyle (
    LifestyleID INT IDENTITY(1,1) PRIMARY KEY,           -- Khóa chính

    SmokerStatus VARCHAR(30) NOT NULL,                   -- Tình trạng hút thuốc: Never, Former, Current(some days)
    ECigaretteUsage VARCHAR(30) NOT NULL,                -- Tình trạng sử dụng vape: Never, Sometimes, Every day
    AlcoholDrinkers VARCHAR(10) NOT NULL,                -- Uống rượu: Yes/No
    SleepQuality VARCHAR(20) NOT NULL                    -- Chất lượng giấc ngủ: Poor, Good, Excessive
);

-- Tạo bảng DIMCHRONICDISEASES: lưu thông tin bệnh mãn tính khác
CREATE TABLE dbo.DimChronicDiseases (
    ChronicDiseaseID INT IDENTITY(1,1) PRIMARY KEY,      -- Khóa chính, tự tăng

    HadDiabetes VARCHAR(3) NOT NULL,                     -- Tiểu đường: Yes/No
    HadArthritis VARCHAR(3) NOT NULL,                    -- Viêm khớp: Yes/No
    HadCOPD VARCHAR(3) NOT NULL,                         -- Bệnh phổi tắc nghẽn mạn tính: Yes/No
    HadKidneyDisease VARCHAR(3) NOT NULL,                -- Bệnh thận: Yes/No
    HadDepressiveDisorder VARCHAR(3) NOT NULL,           -- Rối loạn trầm cảm: Yes/No
    HadCancer VARCHAR(3) NOT NULL,                       -- Ung thư: Yes/No
    HadSkinCancer VARCHAR(3) NOT NULL                    -- Ung thư da: Yes/No
);