-- WSJ 数据采集数据库表结构 (PostgreSQL)
-- 用法: psql -U postgres -d wsj_new -f schema.sql

-- 1. Daily_Articles
CREATE TABLE IF NOT EXISTS Daily_Articles (
    id SERIAL PRIMARY KEY,
    Year INTEGER NOT NULL,
    Month INTEGER NOT NULL,
    Date TEXT NOT NULL,
    Article_Title TEXT NOT NULL,
    Article_URL TEXT NOT NULL,
    scrape_time TEXT NOT NULL,
    UNIQUE(Date, Article_URL)
);
CREATE INDEX IF NOT EXISTS idx_daily_articles_date ON Daily_Articles(Date);
CREATE INDEX IF NOT EXISTS idx_daily_articles_year_month ON Daily_Articles(Year, Month);

-- 2. Article_Info
CREATE TABLE IF NOT EXISTS Article_Info (
    Art_ID TEXT PRIMARY KEY,
    Art_Title TEXT NOT NULL,
    Art_Title_Short TEXT,
    Art_Author TEXT,
    Art_Time TEXT,
    Art_Tag_1 TEXT,
    Art_Tag_2 TEXT,
    Comments_Count INTEGER DEFAULT 0,
    Art_URL TEXT NOT NULL,
    Art_Text TEXT,
    Art_Text_Html TEXT,
    scrape_time TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_article_info_time ON Article_Info(Art_Time);
CREATE INDEX IF NOT EXISTS idx_article_info_comments ON Article_Info(Comments_Count);

-- 3. Comment_Info
CREATE TABLE IF NOT EXISTS Comment_Info (
    id SERIAL PRIMARY KEY,
    Article_ID TEXT NOT NULL,
    Comment_ID TEXT NOT NULL,
    Reply_2_Comment_ID TEXT,
    Comment_Text TEXT NOT NULL,
    Comment_Time TEXT,
    Cmt_Likes_Count INTEGER DEFAULT 0,
    Cmt_Likes_User_ID TEXT[],
    Cmt_Likes_User_Nm TEXT[],
    User_ID TEXT NOT NULL,
    User_Nm TEXT NOT NULL,
    scrape_time TEXT NOT NULL,
    UNIQUE(Article_ID, Comment_ID)
);
CREATE INDEX IF NOT EXISTS idx_comment_article ON Comment_Info(Article_ID);
CREATE INDEX IF NOT EXISTS idx_comment_user ON Comment_Info(User_ID);
CREATE INDEX IF NOT EXISTS idx_comment_reply ON Comment_Info(Reply_2_Comment_ID);

-- 4. User_Info
CREATE TABLE IF NOT EXISTS User_Info (
    User_ID TEXT PRIMARY KEY,
    User_Nm TEXT NOT NULL,
    User_Posts INTEGER DEFAULT 0,
    User_Likes INTEGER DEFAULT 0,
    User_Url TEXT,
    scrape_time TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_user_info_nm ON User_Info(User_Nm);

-- 5. User_Post_Info
CREATE TABLE IF NOT EXISTS User_Post_Info (
    id SERIAL PRIMARY KEY,
    User_ID TEXT NOT NULL,
    User_Nm TEXT NOT NULL,
    Post_Art_Title TEXT,
    Post_Art_ID TEXT,
    Post_Text TEXT NOT NULL,
    Post_Rply TEXT,
    Rply2_User_ID TEXT,
    Rply2_User_Nm TEXT,
    Post_Time TEXT,
    scrape_time TEXT NOT NULL,
    UNIQUE(User_ID, Post_Art_ID, Post_Text, Post_Time)
);
CREATE INDEX IF NOT EXISTS idx_user_post_user ON User_Post_Info(User_ID);
CREATE INDEX IF NOT EXISTS idx_user_post_article ON User_Post_Info(Post_Art_ID);

-- 6. scrape_progress
CREATE TABLE IF NOT EXISTS scrape_progress (
    key TEXT PRIMARY KEY,
    value TEXT
);
