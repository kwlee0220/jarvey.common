package jarvey;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.concurrent.locks.LockSupport;

import org.apache.hadoop.conf.Configuration;

import com.google.common.base.Objects;

import utils.stream.FStream;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class LfsPath implements FilePath, Serializable {
	private static final long serialVersionUID = 1L;
	
	private transient File m_file;
	
	public static LfsPath of(File file) {
		return new LfsPath(file);
	}
	
	public static LfsPath of(String path) {
		return new LfsPath(new File(path));
	}
	
	LfsPath(File file) {
		m_file = file;
	}
	
	public File getFile() {
		return m_file;
	}

	@Override
	public String getName() {
		return m_file.getName();
	}

	@Override
	public String getPath() {
		return m_file.getPath();
	}

	@Override
	public String getAbsolutePath() {
		return m_file.getAbsolutePath();
	}

	@Override
	public boolean exists() {
		return m_file.exists();
	}

	@Override
	public LfsPath getParent() {
		File parent = m_file.getParentFile();
		return parent != null ? LfsPath.of(parent) : null;
	}

	@Override
	public LfsPath getChild(String childName) {
		return LfsPath.of(new File(m_file, childName));
	}

	@Override
	public boolean isDirectory() {
		return m_file.isDirectory();
	}

	@Override
	public boolean isRegular() {
		String fname = getName();
		return !(fname.startsWith("_") || fname.startsWith("."));
	}

	@Override
	public FStream<FilePath> streamChildFilePaths() {
		File[] subFiles = m_file.listFiles();
		if ( subFiles != null ) {
			return FStream.of(subFiles)
							.map(file -> LfsPath.of(file))
							.cast(FilePath.class);
		}
		else {
			return null;
		}
	}

	@Override
	public boolean delete() {
		return m_file.delete();
	}

	@Override
	public void renameTo(FilePath dstFile, boolean replaceExisting) throws IOException {
		if ( !exists() ) {
			throw new IOException("source not found: file=" + m_file);
		}
		
		if ( !(dstFile instanceof LfsPath) ) {
			throw new IllegalArgumentException("incompatible destination file handle: " + dstFile);
		}
		LfsPath dst = (LfsPath)dstFile;
		
		FilePath parent = dstFile.getParent();
		parent.mkdirs();
		
		if ( replaceExisting ) {
	        Files.move(m_file.toPath(), dst.m_file.toPath(), StandardCopyOption.REPLACE_EXISTING);
		}
		else {
			if ( dst.exists() ) {
				throw new IOException("destination exists: file=" + dstFile);
			}
	        Files.move(m_file.toPath(), dst.m_file.toPath());
		}
	}

	@Override
	public boolean mkdirs() {
		if ( exists() ) {
			return false;
		}
		
		LfsPath parent = getParent();
		if ( parent != null ) {
			if ( !parent.exists() ) {
				parent.mkdirs();
			}
			else if ( !parent.isDirectory() ) {
				return false;
			}
		}
		
		return m_file.mkdir();
	}

	@Override
	public long getLength() throws IOException {
        // Reading the file length is a tricky business.
        // We will retry some.
        Exception lastError = null;
        for (int trialIndex = 0; trialIndex < 5; trialIndex++) {
            long fileLength = m_file.length();
            if (fileLength != 0) {
                return fileLength;
            }
            // `File#length()` can return 0 due to I/O failures.
            // We are falling back to NIO for a second attempt.
            else {
                Path path = m_file.toPath();
                try {
                    return Files.size(path);
                } catch (IOException error) {
                    lastError = error;
                }
            }
            // Scientifically proven retry practice: wait a bit.
            LockSupport.parkNanos(1);
        }
        
        String message = String.format("file length read failure {file=%s}", m_file);
        throw new IOException(message, lastError);
    }

	@Override
	public LfsPath path(String path) {
		return LfsPath.of(new File(path));
	}

	@Override
	public FileInputStream read() throws IOException {
		return new FileInputStream(m_file);
	}

	@Override
	public OutputStream create(boolean overwrite) throws IOException {
		FilePath parent = getParent();
		if ( parent != null ) {
			parent.mkdirs();
		}
		
		return new FileOutputStream(m_file, false);
	}

	@Override
	public OutputStream append() throws IOException {
		FilePath parent = getParent();
		if ( parent != null ) {
			parent.mkdirs();
		}
		
		return new FileOutputStream(m_file, true);
	}
	
	@Override
	public String toString() {
		return m_file.toString();
	}
	
	@Override
	public boolean equals(Object obj) {
		if ( this == obj ) {
			return true;
		}
		else if ( this == null || getClass() != LfsPath.class ) {
			return false;
		}
		
		LfsPath other = (LfsPath)obj;
		return Objects.equal(m_file, other.m_file);
	}
	
	private void writeObject(ObjectOutputStream os) throws IOException {
		os.defaultWriteObject();
		
		os.writeUTF(m_file.getAbsolutePath());
	}
	
	private void readObject(ObjectInputStream is) throws IOException, ClassNotFoundException {
		is.defaultReadObject();
		
		Configuration conf = new Configuration();
		conf.readFields(is);
		
		m_file = new File(is.readUTF());
	}
}
