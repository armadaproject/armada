import nextEnv from '@next/env';
import {
  existsSync,
  lstatSync,
  symlinkSync,
  unlinkSync,
  readdirSync,
} from 'node:fs';
import http from 'node:http';
import path from 'node:path';
import handler from 'serve-handler';

/**
 * Pure function to validate base path format
 * @param {string} basePath - The base path to validate
 * @returns {string} The validated base path (empty string if falsy input)
 * @throws {Error} If base path format is invalid
 */
const parseBasePath = (basePath) => {
  if (!basePath) return ``;

  if (!basePath.startsWith('/') || basePath.endsWith('/')) {
    throw new Error(
      "NEXT_PUBLIC_BASE_PATH must be '' or '/subpath' (without trailing slash)."
    );
  }

  return basePath;
};

/**
 * Pure function to derive subdirectory name from base path
 * @param {string} basePath - The base path (e.g., '/subpath')
 * @returns {string} The subdirectory name (e.g., 'subpath')
 */
const parseSubDirName = (basePath) => (basePath ? basePath.slice(1) : ``);

/**
 * Pure function to validate file or directory existence and type
 * @param {string} targetPath - The path to validate
 * @param {boolean} [isDir=false] - Whether to check if target is a directory
 * @param {boolean} [isSymlink=false] - Whether to check if target is a symbolic link
 * @returns {boolean} True if validation passes
 * @throws {Error} If validation fails
 */
const validatePath = (targetPath, isDir = false, isSymlink = false) => {
  if (!existsSync(targetPath)) {
    let errorMsg;
    if (isDir) {
      errorMsg = `The directory "${targetPath}" does not exist.`;
    } else if (isSymlink) {
      errorMsg = `The symbolic link "${targetPath}" does not exist.`;
    } else {
      errorMsg = `The file "${targetPath}" does not exist. You may need to run 'yarn build' first.`;
    }
    throw new Error(errorMsg);
  }

  if (isDir && !lstatSync(targetPath).isDirectory()) {
    throw new Error(
      `Expected "${targetPath}" to be a directory, but it is not.`
    );
  }

  if (isSymlink && !lstatSync(targetPath).isSymbolicLink()) {
    throw new Error(
      `Expected "${targetPath}" to be a symbolic link, but it is not.`
    );
  }

  return true;
};

/**
 * Function to create a symbolic link to the static directory
 * @param {string} staticPath - The absolute path to the static directory
 * @param {string} subDirName - The subdirectory name to create as a symlink
 * @returns {string} The path to the created symbolic link
 */
const createSymbolicLink = (staticPath, subDirName) => {
  const symlinkPath = path.join(staticPath, subDirName);

  if (existsSync(symlinkPath)) {
    if (!lstatSync(symlinkPath).isSymbolicLink()) {
      throw new Error(
        `A file or directory already exists at "${symlinkPath}" that is not a symbolic link.`
      );
    }
    unlinkSync(symlinkPath);
    console.log(`Deleted existing symbolic link: ${symlinkPath}`);
  }

  symlinkSync(staticPath, symlinkPath, 'dir');
  console.log(`Created symbolic link: ${symlinkPath} -> ${staticPath}`);
  return symlinkPath;
};

/**
 * Function to find all symbolic links in a directory
 * @param {string} dirPath - The directory to search for symbolic links
 * @return {string[]} Array of symbolic link paths
 */
const findSymbolicLinks = (dirPath) => {
  return readdirSync(dirPath, { withFileTypes: true })
    .filter((dirent) => dirent.isSymbolicLink())
    .map((dirent) => path.join(dirent.parentPath, dirent.name));
};

/**
 * Main execution function that orchestrates the server setup and startup
 * @returns {void} Promise that resolves when server is started
 */
const main = () => {
  const projectPath = process.cwd();
  console.log(`projectDir: ${projectPath}`);

  // Load environment configuration
  nextEnv.loadEnvConfig(projectPath);

  // static directory
  const staticDirName = 'out';
  console.log(`Using static directory: ${staticDirName}`);
  const staticPath = path.join(projectPath, staticDirName);

  // base path
  const basePath = parseBasePath(process.env.NEXT_PUBLIC_BASE_PATH || '');
  console.log(`Using base path: ${basePath || '(none)'}`);

  // subdirectory name
  const subDirName = parseSubDirName(basePath);

  // Validate the static directory exists and is a directory
  validatePath(staticPath, true, false);

  if (subDirName) {
    // if basePath is set, we need to create a symbolic link
    const symlinkPath = createSymbolicLink(staticPath, subDirName);
    validatePath(symlinkPath, false, true);
  } else {
    // if basePath is not set, we check that index.html exists in the static directory
    const indexPath = path.join(staticPath, 'index.html');
    validatePath(indexPath);
    // delete any existing symbolic links in the static directory
    const symlinks = findSymbolicLinks(staticPath);
    symlinks.forEach((link) => {
      unlinkSync(link);
      console.log(`Deleted symbolic link: ${link}`);
    });
  }

  const server = http.createServer((request, response) => {
    return handler(request, response, {
      // mimic GitHub Pages behavior
      public: staticDirName,
      directoryListing: false,
      cleanUrls: true,
      trailingSlash: !!basePath, // if basePath is set, we want to keep the trailing slash
    });
  });

  server.listen(3000, () => {
    console.log('Running at http://localhost:3000');
  });
};

// Start the application
main();
