package aitu;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.fs.permission.FsPermission;

public class NoPermLocalFileSystem extends RawLocalFileSystem {
    @Override
    public void setPermission(Path p, FsPermission permission) throws IOException {
    }

    @Override
    public void setOwner(Path p, String username, String groupname) throws IOException {
    }

    @Override
    public FileStatus getFileStatus(Path f) throws IOException {
        File file = pathToFile(f);
        if (!file.exists()) {
            throw new FileNotFoundException("File " + f + " does not exist");
        }
        boolean dir = file.isDirectory();
        return new FileStatus(dir ? 0 : file.length(), dir, 1, getDefaultBlockSize(f),
                file.lastModified(), 0, new FsPermission(dir ? (short) 0755 : (short) 0644),
                "", "", makeQualified(f));
    }

    @Override
    public FileStatus getFileLinkStatus(Path f) throws IOException {
        return getFileStatus(f);
    }

    @Override
    public FileStatus[] listStatus(Path f) throws IOException {
        File file = pathToFile(f);
        if (!file.exists()) {
            throw new FileNotFoundException("File " + f + " does not exist");
        }
        if (file.isFile()) {
            return new FileStatus[] {getFileStatus(f)};
        }
        String[] names = file.list();
        if (names == null) {
            throw new IOException("Can not list " + f);
        }
        List<FileStatus> out = new ArrayList<>();
        for (String name : names) {
            try {
                out.add(getFileStatus(new Path(f, name)));
            } catch (FileNotFoundException e) {
            }
        }
        return out.toArray(new FileStatus[0]);
    }
}
