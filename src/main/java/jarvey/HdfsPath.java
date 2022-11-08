package jarvey;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.io.UncheckedIOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.google.common.base.Objects;

import utils.io.FilePath;
import utils.stream.FStream;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class HdfsPath implements FilePath, Serializable {
	private static final long serialVersionUID = 1L;
	
	private transient FileSystem m_fs;
	private transient Path m_path;
	
	public static HdfsPath of(FileSystem fs, Path path) {
		return new HdfsPath(fs, path);
	}
	
	public static HdfsPath of(FileSystem fs, String path) {
		return new HdfsPath(fs, new Path(path));
	}
	
	private HdfsPath(FileSystem fs, Path path) {
		m_fs = fs;
		m_path = path;
	}

	@Override
	public String getName() {
		return m_path.getName();
	}

	@Override
	public String getPath() {
		return m_path.toString();
	}
	
	public FileSystem getFileSystem() {
		return m_fs;
	}

	public FileStatus getFileStatus() throws IOException {
		return m_fs.getFileStatus(m_path);
	}

	@Override
	public synchronized String getAbsolutePath() {
		try {
			if ( !m_path.isAbsolute() || m_path.isAbsoluteAndSchemeAuthorityNull() ) {
				FileStatus current = m_fs.getFileStatus(new Path("."));
				m_path = new Path(current.getPath(), m_path);
			}
			
			return m_path.toString();
		}
		catch ( IOException e ) {
			throw new UncheckedIOException(e);
		}
	}

	@Override
	public HdfsPath getParent() {
		Path parent = m_path.getParent();
		return parent != null ? HdfsPath.of(m_fs, parent) : null;
	}

	@Override
	public HdfsPath getChild(String childName) {
		return HdfsPath.of(m_fs, new Path(m_path, childName));
	}

	@Override
	public boolean isDirectory() {
		try {
			return m_fs.getFileStatus(m_path).isDirectory();
		}
		catch ( IOException e ) {
			throw new UncheckedIOException(e);
		}
	}

	@Override
	public boolean isRegular() {
		String fname = getName();
		return !(fname.startsWith("_") || fname.startsWith("."));
	}

	@Override
	public FStream<FilePath> streamChildFilePaths() throws IOException {
		return FStream.of(m_fs.listStatus(m_path))
						.map(s -> of(m_fs, s.getPath()))
						.cast(FilePath.class);
	}

	@Override
	public long getLength() throws IOException {
		return m_fs.getFileStatus(m_path).getLen();
    }

	@Override
	public boolean exists() {
		try {
			return m_fs.exists(m_path);
		}
		catch ( IOException e ) {
			throw new UncheckedIOException(e);
		}
	}

	@Override
	public boolean delete() {
		try {
			if ( !exists() ) {
				return true;
			}
			
			return m_fs.delete(m_path, true);
		}
		catch ( IOException e ) {
			return false;
		}
	}

	@Override
	public void renameTo(FilePath dstFile, boolean replaceExisting) throws IOException {
		if ( !exists() ) {
			throw new IOException("source not found: file=" + m_path);
		}
		
		if ( !(dstFile instanceof HdfsPath) ) {
			throw new IllegalArgumentException("incompatible destination file handle: " + dstFile);
		}
		HdfsPath dst = (HdfsPath)dstFile;
		
		boolean dstExists = dst.exists();
		if ( replaceExisting && dstExists ) {
			throw new IOException("destination exists: file=" + dstFile);
		}

		HdfsPath parent = dst.getParent();
		if ( parent != null ) {
			if ( !parent.exists() ) {
				parent.mkdirs();
			}
			else if ( !parent.isDirectory() ) {
				throw new IOException("destination's parent is not a directory: path=" + parent);
			}
		}
		if ( !replaceExisting && dstExists ) {
			dst.delete();
		}
		
		if ( !m_fs.rename(m_path, dst.m_path) ) {
			throw new IOException("fails to rename to " + dst);
		}

		// 파일 이동 후, 디렉토리가 비게 되면 해당 디렉토리를 올라가면서 삭제한다.
		if ( parent != null ) {
			parent.deleteIfEmptyDirectory();
		}
	}

	@Override
	public boolean mkdirs() {
		if ( exists() ) {
			return false;
		}
		
		HdfsPath parent = getParent();
		if ( parent != null ) {
			if ( !parent.exists() ) {
				parent.mkdirs();
			}
			else if ( !parent.isDirectory() ) {
				throw new UncheckedIOException(new IOException("the parent file is not a directory"));
			}
		}
		
		try {
			return m_fs.mkdirs(m_path);
		}
		catch ( IOException e ) {
			throw new UncheckedIOException(e);
		}
	}

	@Override
	public HdfsPath path(String path) {
		return HdfsPath.of(m_fs, new Path(path));
	}

	@Override
	public FSDataInputStream read() throws IOException {
		return m_fs.open(m_path);
	}

	@Override
	public FSDataOutputStream create(boolean overwrite) throws IOException {
		FilePath parent = getParent();
		if ( parent != null ) {
			parent.mkdirs();
		}
		
		return m_fs.create(m_path, overwrite);
	}
	
	public FSDataOutputStream create(boolean overwrite, long blockSize) throws IOException {
		int bufferSz = m_fs.getConf()
							.getInt(CommonConfigurationKeysPublic.IO_FILE_BUFFER_SIZE_KEY,
									CommonConfigurationKeysPublic.IO_FILE_BUFFER_SIZE_DEFAULT);
		FilePath parent = getParent();
		if ( parent != null ) {
			parent.mkdirs();
		}
		
		return m_fs.create(m_path, overwrite, bufferSz,
							m_fs.getDefaultReplication(m_path), blockSize);
	}

	@Override
	public FSDataOutputStream append() throws IOException {
		FilePath parent = getParent();
		if ( parent != null ) {
			parent.mkdirs();
		}
		
		return m_fs.append(m_path);
	}
	
	@Override
	public String toString() {
		return m_path.toString();
	}
	
	@Override
	public boolean equals(Object obj) {
		if ( this == obj ) {
			return true;
		}
		else if ( this == null || getClass() != HdfsPath.class ) {
			return false;
		}
		
		HdfsPath other = (HdfsPath)obj;
		return Objects.equal(m_path, other.m_path);
	}
	
	private void writeObject(ObjectOutputStream os) throws IOException {
		os.defaultWriteObject();
		
		m_fs.getConf().write(os);
		os.writeUTF(m_path.toString());
	}
	
	private void readObject(ObjectInputStream is) throws IOException, ClassNotFoundException {
		is.defaultReadObject();
		
		Configuration conf = new Configuration();
		conf.readFields(is);
		
		m_fs = FileSystem.get(conf);
		m_path = new Path(is.readUTF());
	}
}
