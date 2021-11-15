package io.chubao.fs;

import io.chubao.fs.CfsLibrary.Dirent;

import java.io.FileNotFoundException;

public class TestCfsClient {
    public static void main(String[] args) throws FileNotFoundException {
        CfsMount mnt = new CfsMount();

        mnt.setClient("volName", "hot_lee");
        mnt.setClient("masterAddr", "10.177.69.105:17010,10.177.69.106:17010,10.177.117.108:17010");
        mnt.setClient("logDir", "/home/ADC/80301920/Documents/logs");
        mnt.setClient("logLeval", "debug");
        mnt.setClient("accessKey", "cCwD");
        mnt.setClient("secretKey", "dicRzKnr");
        mnt.setClient("enableSummary", "true");
        int ret = mnt.startClient();
        if (ret < 0) {
            System.out.println("start client failed!!!");
            return;
        }

        if (args.length < 2) {
            System.out.println("Invalid args: lack of path");
            return;
        }

        String targetPath = args[1];

        if (args[0].equals("read")) {
            CfsLibrary.StatInfo stat = new CfsLibrary.StatInfo();
            ret = mnt.getAttr(targetPath, stat);
            if (ret < 0) {
                System.out.println("GetAttr failed: " + ret);
                return;
            }

            System.out.println("ino: " + stat.ino);
            System.out.println("size: " + stat.size);
            System.out.println("blocks: " + stat.blocks);
            System.out.println("nlink: " + stat.nlink);
            System.out.println("mode: " + stat.mode);
            System.out.println("uid: " + stat.uid);
            System.out.println("gid: " + stat.gid);

            int fd = mnt.open(targetPath, CfsMount.O_RDONLY, 0644);

            if (fd < 0) {
                System.out.println("Open failed: " + fd);
                return;
            }

            byte[] buf = new byte[4096];

            long readByte = mnt.read(fd, buf, buf.length, 0);

            System.out.println("fd: " + fd);
            System.out.println("read bytes: " + readByte);
            System.out.println(new String(buf));

            // verify md5sum
            byte[] toVerify = new byte[(int) readByte];
            System.arraycopy(buf, 0, toVerify, 0, (int) readByte);

            StringBuilder sb = new StringBuilder();
            java.security.MessageDigest md5 = null;
            try {
                md5 = java.security.MessageDigest.getInstance("MD5");
                md5.update(toVerify);
            } catch (java.security.NoSuchAlgorithmException e) {
            }
            if (md5 != null) {
                for (byte b : md5.digest()) {
                    sb.append(String.format("%02x", b));
                }
            }

            System.out.println("md5: " + sb.toString());

            mnt.close(fd);
        } else if (args[0].equals("write")) {
            int fd = mnt.open(targetPath, CfsMount.O_RDWR | CfsMount.O_CREAT, 0644);

            if (fd < 0) {
                System.out.println("Open failed: " + fd);
                return;
            }

            String toWrite = "abcdefg";
            byte[] buf = toWrite.getBytes();

            long writeBytes = mnt.write(fd, buf, buf.length, 0);

            System.out.println("write bytes: " + writeBytes);

            mnt.close(fd);
        } else if (args[0].equals("ls")) {
            mnt.chdir(targetPath);
            int fd = mnt.open(".", CfsMount.O_RDWR, 0644);

            if (fd < 0) {
                System.out.println("Open failed: " + fd);
                return;
            }

            int total_entry = 0;
            Dirent dent = new Dirent();
            Dirent[] dents = (Dirent[]) dent.toArray(2);
            for (;;) {
                int n = mnt.readdir(fd, dents, 2);
                if (n <= 0) {
                    break;
                }
                total_entry += n;
                for (int i = 0; i < n; i++) {
                    System.out.println("ino: " + dents[i].ino + " | d_type: " + dents[i].dType);
                }
            }
            System.out.println("num of entries: " + total_entry);
            mnt.close(fd);
        } else if (args[0].equals("chmod")) {
            int fd = mnt.open(targetPath, CfsMount.O_RDWR, 0644);
            if (fd < 0) {
                System.out.println("Open failed: " + fd);
                return;
            }

            ret = mnt.fchmod(fd, 0666);
            if (ret < 0) {
                System.out.println("Fchmod failed: " + ret);
                return;
            }
            CfsLibrary.StatInfo stat = new CfsLibrary.StatInfo();
            ret = mnt.getAttr(targetPath, stat);
            System.out.println("mode: " + stat.mode);
            mnt.close(fd);
        } else if (args[0].equals("summary")) {
            CfsLibrary.SummaryInfo.ByReference summaryInfo = new CfsLibrary.SummaryInfo.ByReference();
            ret = mnt.getsummary(targetPath, summaryInfo, 10);
            if (ret < 0) {
                System.out.println("Getsummary failed: " + ret);
                return;
            }
            System.out.println("files: " + summaryInfo.rfiles);
            System.out.println("subdirs: " + summaryInfo.rsubdirs);
            System.out.println("bytes: " + summaryInfo.rbytes);
        }

        mnt.closeClient();
    }
}
