require 'rdbi/driver/rubyfb'
require 'typelib'
require 'rubyfb'
require 'date'
require 'epoxy'

#--
# TODO:  Allow changing time zone
#
# AutoCommit considerations: delay commit for SELECT, SELECT FOR UPDATE, and
# EXECUTE PROC statements.
#++

class RDBI::Driver::Rubyfb
class Statement < RDBI::Statement
  # Type conversions we perform:
  #
  #   Firebird    Rubyfb     RDBI    Notes
  #   ---------  --------  --------  -------------------------------------
  #   TIMESTAMP      Time  DateTime
  #   TIMESTAMP  DateTime  DateTime  (if out of range of Time)
  #        CHAR     'a   '      'a'
  #
  RTRIM_RE   = ::Regexp.new(/ +\z/)                 # :nodoc:
  TIME_ZONE  = ::DateTime.now.zone                  # :nodoc:
  STR_RTRIM  = proc { |str| str.sub(RTRIM_RE, '') } # :nodoc:
  IS_STR     = proc { |x| x.kind_of?(::String) }    # :nodoc:
  IS_TIME    = proc { |x| x.kind_of?(::Time) }      # :nodoc:
  TIME_TO_DT = proc { |t|                           # :nodoc:
                 ::DateTime.new(t.year,
                                t.month,
                                t.day,
                                t.hour,
                                t.min,
                                t.sec + Rational(t.usec, 10**6),
                                Rational(t.utc_offset, 60 * 60 * 24))
               } # :nodoc:

  OUTPUT_MAP = RDBI::Type.create_type_hash(RDBI::Type::Out).merge({ # :nodoc:
                 :timestamp => [TypeLib::Filter.new(IS_TIME, TIME_TO_DT)],
                 :char      => [TypeLib::Filter.new(IS_STR, STR_RTRIM)]
               }) # :nodoc:

  def initialize(query, dbh, fb_stmt)
    super(query, dbh)

    @fb_stmt = fb_stmt

    @index_map = Epoxy.new(query).indexed_binds
  end

  def finish
    #puts "finishing #{@fb_stmt}"
    @fb_stmt.close
    super
  end

  def new_modification(*binds)
    new_execution(*binds)
  end

  def new_execution(*binds)
    hashes, binds = binds.partition { |x| x.kind_of?(Hash) }
    hash = hashes.inject({}) { |x, y| x.merge(y) }
    hash.keys.each do |key|
      if index = @index_map.index(key)
        binds.insert(index, hash[key])
      end
    end

    result = binds.length > 0 ? @fb_stmt.execute_for(binds) : @fb_stmt.execute

    num_columns = result.column_count rescue 0
    columns = (0...num_columns).collect do |i|
      base_type = result.get_base_type(i).to_s.downcase.to_sym
      ruby_type = Types::rubyfb_to_rdbi(base_type,
                                        (result.column_scale(i) rescue 0))
      c = RDBI::Column.new(
                           result.column_alias(i).to_sym,
                           base_type,
                           ruby_type,
                           0,
                           0
                          )
                          #puts c
                          #c
    end
    cursor_klass = self.rewindable_result ? ArrayCursor : ForwardOnlyCursor
    [ cursor_klass.new(result), RDBI::Schema.new(columns), OUTPUT_MAP ]
  end #-- new_execution

end #-- class Statement
class AutoCommitStatement < Statement
  def initialize(query, dbh, fb_stmt)
    super
  end

  def new_execution(*binds)
    cursor, col_info, type_map = super

    if commit_immediately?
      @fb_stmt.transaction.commit if commit_immediately?
    else
      (class << cursor.handle; self; end).class_eval do
        def finish
          transaction.commit if transaction.active?
          super
        end
      end
    end
  rescue
    @fb_stmt.transaction.rollback if @fb_stmt.transaction.active?
  end

  def finish
    @fb_stmt.transaction.commit if @fb_stmt.transaction.active?
    super
  end

  private
  def commit_immediately?
    self.rewindable_result or not
      [Rubyfb::Statement::SELECT_STATEMENT,
       Rubyfb::Statement::SELECT_FOR_UPDATE_STATEMENT,
       Rubyfb::Statement::EXECUTE_PROCEDURE_STATEMENT].include?(@fb_stmt.type)
  end
end #-- class AutoCommitStatement
end #-- class RDBI::Driver::Rubyfb
