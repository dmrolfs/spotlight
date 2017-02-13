object Format {
  import com.typesafe.sbt.SbtScalariform._

  lazy val settings = defaultScalariformSettings ++ Seq(
    ScalariformKeys.preferences := formattingPreferences
  )

  lazy val formattingPreferences = {
    import scalariform.formatter.preferences._
    FormattingPreferences().
      setPreference( DoubleIndentClassDeclaration, true ).
      setPreference( MultilineScaladocCommentsStartOnFirstLine, true ).
      setPreference( PlaceScaladocAsterisksBeneathSecondAsterisk, true ).
      // setPreference( FirstParameterOnNewline, "Force" ).
      // setPreference( DanglingCloseParenthesis, "Force").
      setPreference( RewriteArrowSymbols, true ).
      setPreference( SpaceInsideParentheses, true ).
      setPreference( SpacesAroundMultiImports, true )
  }
}
