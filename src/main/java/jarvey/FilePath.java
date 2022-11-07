package jarvey;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.function.Function;

import utils.func.UncheckedFunction;
import utils.func.UncheckedPredicate;
import utils.stream.FStream;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public interface FilePath {
	/**
	 * Returns the name of the file or directory denoted by this abstract pathname.
	 *
	 * @return The name of the file or directory denoted by this abstract pathname,
	 * or the empty string if this pathname's name sequence is empty
	 */
	public String getName();
	
	/**
	 * 본 파일의 경로를 반환한다.
	 * 
	 * @return	파일 경로명.
	 */
	public String getPath();
	
	/**
	 * 본 파일의 절대 경로를 반환한다.
	 *
	 * @return 절대 경로.
	 */
	public String getAbsolutePath();
	
	/**
	 * 주어진 경로에 해당하는 proxy 파일을 생성한다.
	 *
	 * @param path	경로명.
	 * @return	proxy 객체.
	 */
	public FilePath path(String path);
	
	/**
	 * 본 파일 proxy에 해당하는 파일의 실제 존재 여부를 반환한다.
	 *
	 * @return	존재하는 경우에는 {@code true}, 그렇지 않은 경우는 {@code false}.
	 * @throws IOException	I/O 오류가 발생된 경우.
	 */
	public boolean exists();
	
	/**
	 * 본 파일의 디렉토리 여부를 반환한다.
	 *
	 * @return	본 proxy에 해당하는 파일이 디렉토리인 경우는 {@code true},
	 * 			그렇지 않은 경우는 {@code false}가 반환됨.
	 */
	public boolean isDirectory();
	
	public boolean isRegular();
	
	public default boolean isRegularFile() {
		return !isDirectory() && isRegular();
	}
	
	/**
	 * Parent 파일 객체를 반환한다.
	 * 만일 parent 파일이 존재하지 않는 경우에는 {@code null}을 반환한다.
	 *
	 * @return	parent 파일 객체 또는 parent 파일이 존재하지 않는 경우에는 {@code null}.
	 */
	public FilePath getParent();
	
	/**
	 * 현 디렉토리의 주어진 이름의 하위 파일 객체를 반환한다. 
	 *
	 * @param childName		파일 이름.
	 * @return	하위 파일 객체.
	 */
	public FilePath getChild(String childName);
	
	/**
	 * 현 디렉토리에 속한 {@link FilePath} 리스트를 반환한다.
	 * 만일 현 파일이 디렉토리가 아닌 경우에는 {@code null}을 반환한다.
	 *
	 * @return	{@link FilePath} 리스트.
	 * @throws IOException	I/O 오류가 발생된 경우.
	 */
	public FStream<FilePath> streamChildFilePaths() throws IOException;
	
	/**
	 * 인자로 주어진 파일 proxy의 경로로 현 파일을 이동시킨다.
	 *
	 * @param dstFile	이동시킬 대상 경로.
	 * @param replaceExisting	대상 경로의 파일이 존재하는 경우, 기존 파일 제거하고 이동할지 여부.
	 * 					{@code true}인 경우는 기존 파일을 제거하고, {@code false}인 경우는 예외를 발생시킴.
	 * @throws IOException	I/O 오류가 발생된 경우.
	 */
	public void renameTo(FilePath dstFile, boolean replaceExisting) throws IOException;
	
	/**
	 * 현 파일 proxy에 해당하는 파일을 삭제한다.
	 *
	 * @return	대상 파일이 삭제된 경우는 {@code true}, 그렇지 않은 경우에는 {@code false}.
	 */
	public boolean delete();
	
	public default void deleteUpward() throws IOException {
		delete();
		
		FilePath parent = getParent();
		if ( parent != null ) {
			parent.delete();
		}
		parent.deleteIfEmptyDirectory();
	}
	
	public default void deleteIfEmptyDirectory() throws IOException {
		if ( isDirectory() && streamChildFilePaths().exists() ) {
			delete();
			
			FilePath parent = getParent();
			if ( parent != null ) {
				parent.deleteIfEmptyDirectory();
			}
		}
	}
	
	/**
	 * 현 파일 proxy에 해당하는 디렉토리를 생성한다.
	 *
	 * @return	본 메소드로 디렉토리가 생성된 경우는 {@code true}가 반환되고,
	 * 			그렇지 않으면 {@code false} 반환됨.
	 * @throws IOException	I/O 오류가 발생된 경우.
	 */
	public boolean mkdirs();
	
	/**
	 * 현 파일 proxy에 해당하는 파일의 길이를 byte 단위로 반환한다.
	 *
	 * @return	파일 byte 크기.
	 * @throws IOException	I/O 오류가 발생된 경우.
	 */
	public long getLength() throws IOException;
	
	public default long getTotalLength() throws IOException {
		return walkRegularFileTree().mapOrIgnore(FilePath::getLength).mapToLong(v -> v).sum();
	}
	
	/**
	 * 현 파일을 읽기 위한 {@link InputStream}을 반환한다.
	 *
	 * @return	입력 스트림 객체.
	 * @throws IOException	I/O 오류가 발생된 경우.
	 */
	public InputStream read() throws IOException;

	/**
	 * 현 경로에 해당하는 파일을 생성하고, 데이터를 추가하기 위한 {@link OutputStream}을 반환한다.
	 * 주어진 경로에 해당하는 파일이 이미 존재하는 경우에는 {@code overwrite} 값에 따라
	 * {@code true}인 경우는 기존 파일을 삭제하고 새 파일을 생성하고, {@code false}인 경우에는
	 * {@code IOException} 예외를 생성한다. 
	 * 
	 * @param overwrite	기존 파일이 존재하는 경우 해당 파일의 삭제 여부.
	 * @throws IOException	I/O 오류가 발생된 경우.
	 */
	public OutputStream create(boolean overwrite) throws IOException;
	
	/**
	 * 현 파일에 데이터를 추가하기 위한 {@link OutputStream}을 반환한다.
	 * 
	 * @return	출력 스트림 객체.
	 * @throws IOException	I/O 오류가 발생된 경우.
	 */
	public OutputStream append() throws IOException;

	public default FStream<FilePath> walkTree(boolean includeCurrent) throws IOException {
		Function<FilePath,FStream<FilePath>> walkDown
			= UncheckedFunction.sneakyThrow(fp ->  fp.isDirectory() ? fp.walkTree(includeCurrent)
																	: FStream.of(fp));
		FStream<FilePath> descendents = streamChildFilePaths().flatMap(walkDown);
		if ( includeCurrent ) {
			return FStream.concat(FStream.of(this), descendents);
		}
		else {
			return descendents;
		}
	}
	
	public default FStream<FilePath> walkRegularFileTree() throws IOException {
		return walkTree(false).filterNot(UncheckedPredicate.sneakyThrow(FilePath::isDirectory));
	}
}
