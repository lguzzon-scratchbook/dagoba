var gulp = require('gulp')
var babel = require('gulp-babel')
var sourcemaps = require('gulp-sourcemaps')

var buildList = ['dagoba.js']

// Define the build task
gulp.task('build', function(done) {
  return gulp.src(buildList)
    .pipe(sourcemaps.init())
    .pipe(babel())
    .pipe(sourcemaps.write('.'))
    .pipe(gulp.dest('build'))
})

// Define the default task using series
gulp.task('default', gulp.series('build'))
